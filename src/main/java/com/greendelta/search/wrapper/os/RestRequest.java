package com.greendelta.search.wrapper.os;

import java.io.IOException;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.sort.SortOrder;

import com.greendelta.search.wrapper.os.Search.OsRequest;

class RestRequest implements OsRequest {

	private final RestHighLevelClient client;
	private final SearchRequest request;

	RestRequest(RestHighLevelClient client, String indexName) {
		this.client = client;
		this.request = new SearchRequest(indexName);
	}

	@Override
	public void setFrom(int from) {
		request.source().from(from);
	}

	@Override
	public void setSize(int size) {
		request.source().size(size);
	}

	@Override
	public void addSort(String field, SortOrder order) {
		request.source().sort(field, order);
	}

	@Override
	public void addAggregation(AggregationBuilder aggregation) {
		request.source().aggregation(aggregation);
	}

	@Override
	public void setQuery(QueryBuilder query) {
		request.source().query(query);
		request.source().trackTotalHits(true);
	}
	
	@Override
	public void addField(String field) {
		request.source().fetchField(field);
	}

	@Override
	public RestResponse execute() throws IOException {
		return new RestResponse(client.search(request, RequestOptions.DEFAULT));
	}

}
