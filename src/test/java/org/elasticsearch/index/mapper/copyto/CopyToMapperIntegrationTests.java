/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper.copyto;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class CopyToMapperIntegrationTests extends ElasticsearchIntegrationTest {


    @Test
    public void testDynamicTemplateCopyTo() throws Exception {
        assertAcked(
                client().admin().indices().prepareCreate("test-idx")
                        .addMapping("doc", createDynamicTemplateMapping())
        );

        int recordCount = between(1, 200);

        for (int i = 0; i < recordCount * 2; i++) {
            client().prepareIndex("test-idx", "doc", Integer.toString(i))
                    .setSource("test_field", "test " + i, "even", i % 2 == 0)
                    .get();
        }
        client().admin().indices().prepareRefresh("test-idx").execute().actionGet();

        SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());
        
        SearchResponse response = client().prepareSearch("test-idx")
                .setQuery(QueryBuilders.termQuery("even", true))
                .addAggregation(AggregationBuilders.terms("test").field("test_field").size(recordCount * 2)
                        .collectMode(aggCollectionMode))
                .addAggregation(AggregationBuilders.terms("test_raw").field("test_field_raw").size(recordCount * 2)
                        .collectMode(aggCollectionMode))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo((long) recordCount));

        assertThat(((Terms) response.getAggregations().get("test")).getBuckets().size(), equalTo(recordCount + 1));
        assertThat(((Terms) response.getAggregations().get("test_raw")).getBuckets().size(), equalTo(recordCount));

    }


    private XContentBuilder createDynamicTemplateMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("doc")
                .startArray("dynamic_templates")

                .startObject().startObject("template_raw")
                .field("match", "*_raw")
                .field("match_mapping_type", "string")
                .startObject("mapping").field("type", "string").field("index", "not_analyzed").endObject()
                .endObject().endObject()

                .startObject().startObject("template_all")
                .field("match", "*")
                .field("match_mapping_type", "string")
                .startObject("mapping").field("type", "string").field("copy_to", "{name}_raw").endObject()
                .endObject().endObject()

                .endArray();
    }

    /**
     * From #8483
     *
     * DELETE /test
     PUT /test/
     {
         "mappings": {
            "test": {
                "dynamic_templates": [
                    {
                        "foo": {
                            "match": "foo*",
                            "mapping": {
                                "type": "object",
                                "properties": {
                                    "one": {
                                        "type": "string",
                                        "copy_to": [
                                            "{name}.two"
                                           ]
                                    },
                                    "two": {
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    }
                ]
            }
         }
     }

     Fails with: MapperParsingException[attempt to copy value to non-existing object [foo.two]]

     PUT /test/test/1
     {
     "foo": { "one": "bar"}
     }

     Succeeds and creates the correct mapping

     PUT /test/test/1
     {
     "foo": { "three": "bar"}
     }

     The first request now works correctly:

     PUT /test/test/1
     {
     "foo": { "one": "bar"}
     }

     */
    @Test
    public void testCopyToAddsFieldsTooLate() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/copyto/test-mapping.json");
        assertAcked(
                client().admin().indices().prepareCreate("test")
                        .addMapping("test", mapping)
        );

        client().prepareIndex("test", "test", Integer.toString(1))
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().startObject("foo").field("one", "bar").endObject().endObject()
                ).get();
    }
}
