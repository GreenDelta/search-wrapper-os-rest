package com.greendelta.search.wrapper.os;

import java.util.ArrayList;
import java.util.List;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.range.ParsedRange;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms.Bucket;

import com.greendelta.search.wrapper.os.Search.OsResponse;

class RestResponse implements OsResponse {

	private final SearchResponse response;

	RestResponse(SearchResponse response) {
		this.response = response;
	}

	@Override
	public SearchHit[] getHits() {
		return response.getHits().getHits();
	}

	@Override
	public long getTotalHits() {
		return response.getHits().getTotalHits().value;
	}

	@Override
	public List<Aggregation> getAggregations() {
		if (response.getAggregations() == null)
			return new ArrayList<>();
		return response.getAggregations().asList();
	}

	@Override
	public List<? extends Bucket> getTermBuckets(Aggregation aggregation) {
		switch (aggregation.getType()) {
		case StringTerms.NAME:
			return ((ParsedStringTerms) aggregation).getBuckets();
		case LongTerms.NAME:
			return ((ParsedLongTerms) aggregation).getBuckets();
		case DoubleTerms.NAME:
			return ((ParsedDoubleTerms) aggregation).getBuckets();
		default:
			return new ArrayList<>();
		}
	}

	public List<? extends org.opensearch.search.aggregations.bucket.range.Range.Bucket> getRangeBuckets(
			Aggregation aggregation) {
		return ((ParsedRange) aggregation).getBuckets();
	}

}