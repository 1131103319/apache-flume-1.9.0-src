/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.interceptor;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Interceptor that extracts matches using a specified regular expression and
 * appends the matches to the event headers using the specified serializers</p>
 * Note that all regular expression matching occurs through Java's built in
 * java.util.regex package</p>. Properties:
 * <p>
 * regex: The regex to use
 * <p>
 * serializers: Specifies the group the serializer will be applied to, and the
 * name of the header that will be added. If no serializer is specified for a
 * group the default {@link RegexExtractorInterceptorPassThroughSerializer} will
 * be used
 * <p>
 * Sample config:
 * <p>
 * agent.sources.r1.channels = c1
 * <p>
 * agent.sources.r1.type = SEQ
 * <p>
 * agent.sources.r1.interceptors = i1
 * <p>
 * agent.sources.r1.interceptors.i1.type = REGEX_EXTRACTOR
 * <p>
 * agent.sources.r1.interceptors.i1.regex = (WARNING)|(ERROR)|(FATAL)
 * <p>
 * agent.sources.r1.interceptors.i1.serializers = s1 s2
 * agent.sources.r1.interceptors.i1.serializers.s1.type = com.blah.SomeSerializer
 * agent.sources.r1.interceptors.i1.serializers.s1.name = warning
 * agent.sources.r1.interceptors.i1.serializers.s2.type =
 *     org.apache.flume.interceptor.RegexExtractorInterceptorTimestampSerializer
 * agent.sources.r1.interceptors.i1.serializers.s2.name = error
 * agent.sources.r1.interceptors.i1.serializers.s2.dateFormat = yyyy-MM-dd
 * </p>
 * <pre>
 * Example 1:
 * </p>
 * EventBody: 1:2:3.4foobar5</p> Configuration:
 * agent.sources.r1.interceptors.i1.regex = (\\d):(\\d):(\\d)
 * </p>
 * agent.sources.r1.interceptors.i1.serializers = s1 s2 s3
 * agent.sources.r1.interceptors.i1.serializers.s1.name = one
 * agent.sources.r1.interceptors.i1.serializers.s2.name = two
 * agent.sources.r1.interceptors.i1.serializers.s3.name = three
 * </p>
 * results in an event with the the following
 *
 * body: 1:2:3.4foobar5 headers: one=>1, two=>2, three=3
 *
 * Example 2:
 *
 * EventBody: 1:2:3.4foobar5
 *
 * Configuration: agent.sources.r1.interceptors.i1.regex = (\\d):(\\d):(\\d)
 * <p>
 * agent.sources.r1.interceptors.i1.serializers = s1 s2
 * agent.sources.r1.interceptors.i1.serializers.s1.name = one
 * agent.sources.r1.interceptors.i1.serializers.s2.name = two
 * <p>
 *
 * results in an event with the the following
 *
 * body: 1:2:3.4foobar5 headers: one=>1, two=>2
 * </pre>
 */
public class RegexExtractorInterceptor implements Interceptor {

    static final String REGEX = "regex";
    static final String SERIALIZERS = "serializers";

    private static final Logger logger = LoggerFactory
            .getLogger(RegexExtractorInterceptor.class);

    private final Pattern regex;
    private final List<NameAndSerializer> serializers;

    private RegexExtractorInterceptor(Pattern regex,
                                      List<NameAndSerializer> serializers) {
        this.regex = regex;
        this.serializers = serializers;
    }

    @Override
    public void initialize() {
        // NO-OP...
    }

    @Override
    public void close() {
        // NO-OP...
    }

    @Override
    public Event intercept(Event event) {
        Matcher matcher = regex.matcher(
                new String(event.getBody(), Charsets.UTF_8));
        Map<String, String> headers = event.getHeaders();
        if (matcher.find()) {
            for (int group = 0, count = matcher.groupCount(); group < count; group++) {
                int groupIndex = group + 1;
                if (groupIndex > serializers.size()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Skipping group {} to {} due to missing serializer",
                                group, count);
                    }
                    break;
                }
                NameAndSerializer serializer = serializers.get(group);
                if (logger.isDebugEnabled()) {
                    logger.debug("Serializing {} using {}", serializer.headerName,
                            serializer.serializer);
                }
                headers.put(serializer.headerName,
                        serializer.serializer.serialize(matcher.group(groupIndex)));
            }
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    public static class Builder implements Interceptor.Builder {

        private Pattern regex;
        private List<NameAndSerializer> serializerList;
        private final RegexExtractorInterceptorSerializer defaultSerializer =
                new RegexExtractorInterceptorPassThroughSerializer();

        @Override
        public void configure(Context context) {
            String regexString = context.getString(REGEX);
            Preconditions.checkArgument(!StringUtils.isEmpty(regexString),
                    "Must supply a valid regex string");
            regex = Pattern.compile(regexString);
            regex.pattern();
            regex.matcher("").groupCount();
            configureSerializers(context);
        }

        private void configureSerializers(Context context) {
            //todo //解析serializers的配置
            String serializerListStr = context.getString(SERIALIZERS);
            Preconditions.checkArgument(!StringUtils.isEmpty(serializerListStr),
                    "Must supply at least one name and serializer");
            //todo //按空格分隔
            String[] serializerNames = serializerListStr.split("\\s+");

            Context serializerContexts =
                    new Context(context.getSubProperties(SERIALIZERS + "."));

            serializerList = Lists.newArrayListWithCapacity(serializerNames.length);
            //todo //对每一个serializers里面的设置进行操作
            for (String serializerName : serializerNames) {
                Context serializerContext = new Context(
                        serializerContexts.getSubProperties(serializerName + "."));
                //todo //获取serializers.x.type的设置，
                // 默认值是DEFAULT,
                // 即org.apache.flume.interceptor.RegexExtractorInterceptorPassThroughSerializer
                String type = serializerContext.getString("type", "DEFAULT");
                //todo ////获取serializers.x.name的设置
                String name = serializerContext.getString("name");
                Preconditions.checkArgument(!StringUtils.isEmpty(name),
                        "Supplied name cannot be empty.");

                if ("DEFAULT".equals(type)) {
                    //todo //生成NameAndSerializer对象，并加入到List<NameAndSerializer>中，
                    // 静态内部类NameAndSerializer是一个包含了
                    // headerName和RegexExtractorInterceptorSerializer属性的容器，
                    // 这里每一个serializers.x.name的配置对应一个RegexExtractorInterceptorSerializer对象
                    serializerList.add(new NameAndSerializer(name, defaultSerializer));
                } else {
                    //todo //getCustomSerializer用于根据type的设置返回RegexExtractorInterceptorSerializer对象
                    serializerList.add(new NameAndSerializer(name, getCustomSerializer(
                            type, serializerContext)));
                }
            }
        }

        private RegexExtractorInterceptorSerializer getCustomSerializer(
                String clazzName, Context context) {
            try {
                RegexExtractorInterceptorSerializer serializer = (RegexExtractorInterceptorSerializer) Class
                        .forName(clazzName).newInstance();
                serializer.configure(context);
                return serializer;
            } catch (Exception e) {
                logger.error("Could not instantiate event serializer.", e);
                Throwables.propagate(e);
            }
            return defaultSerializer;
        }

        @Override
        public Interceptor build() {
            Preconditions.checkArgument(regex != null,
                    "Regex pattern was misconfigured");
            Preconditions.checkArgument(serializerList.size() > 0,
                    "Must supply a valid group match id list");
            return new RegexExtractorInterceptor(regex, serializerList);
        }
    }

    static class NameAndSerializer {
        private final String headerName;
        private final RegexExtractorInterceptorSerializer serializer;

        public NameAndSerializer(String headerName,
                                 RegexExtractorInterceptorSerializer serializer) {
            this.headerName = headerName;
            this.serializer = serializer;
        }
    }
}
