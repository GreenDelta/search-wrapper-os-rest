package com.greendelta.search.wrapper.os;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import com.greendelta.search.wrapper.SearchClient;
import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.SearchResult;

public class OsRestClient implements SearchClient {

	private final RestHighLevelClient client;
	private final String indexName;

	public OsRestClient(RestHighLevelClient client, String indexName) {
		this.client = client;
		this.indexName = indexName;
	}

	@Override
	public SearchResult<Map<String, Object>> search(SearchQuery searchQuery) {
		try {
			var request = new RestRequest(client, indexName);
			return Search.run(request, searchQuery);
		} catch (Exception e) {
			e.printStackTrace();
			return new SearchResult<>();
		}
	}

	@Override
	public Set<String> searchIds(SearchQuery searchQuery) {
		var request = new RestRequest(client, indexName);
		return Search.ids(request, searchQuery);
	}

	@Override
	public void create(Map<String, String> settings) {
		try {
			var exists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
			if (exists)
				return;
			var config = settings.get("config");
			var mapping = settings.get("mapping");
			CreateIndexRequest request = new CreateIndexRequest(indexName)
					.settings(Settings.builder().loadFromSource(config, XContentType.JSON))
					.mapping(mapping, XContentType.JSON);
			client.indices().create(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void index(String id, Map<String, Object> content) {
		try {
			client.index(indexRequest(id, content, true), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void index(Map<String, Map<String, Object>> contentsById) {
		bulk(request -> contentsById.keySet()
				.forEach(id -> request.add(indexRequest(id, contentsById.get(id), false))));
	}

	private IndexRequest indexRequest(String id, Map<String, Object> content, boolean refresh) {
		var request = new IndexRequest(indexName).id(id).opType(OpType.INDEX).source(content);
		if (refresh) {
			request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return request;
	}

	@Override
	public void update(String id, Map<String, Object> update) {
		try {
			client.update(updateRequest(id, update, true), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void update(String id, String script, Map<String, Object> parameters) {
		try {
			client.update(updateRequest(id, script, parameters, true), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void update(Set<String> ids, Map<String, Object> update) {
		bulk(request -> ids.forEach(id -> request.add(updateRequest(id, update, false))));

	}

	@Override
	public void update(Set<String> ids, String script, Map<String, Object> parameters) {
		bulk(request -> ids.forEach(id -> request.add(updateRequest(id, script, parameters, false))));
	}

	@Override
	public void update(Map<String, Map<String, Object>> updatesById) {
		bulk(request -> updatesById.keySet().forEach(id -> request.add(updateRequest(id, updatesById.get(id), false))));

	}

	private UpdateRequest updateRequest(String id, Map<String, Object> content, boolean refresh) {
		var request = new UpdateRequest(indexName, id).doc(content);
		if (refresh) {
			request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return request;
	}

	private UpdateRequest updateRequest(String id, String script, Map<String, Object> parameters, boolean refresh) {
		var request = new UpdateRequest(indexName, id)
				.script(new Script(ScriptType.INLINE, "painless", script, parameters));
		if (refresh) {
			request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return request;
	}

	@Override
	public void remove(String id) {
		try {
			client.delete(deleteRequest(id, true), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void remove(Set<String> ids) {
		bulk(request -> ids.forEach(id -> request.add(deleteRequest(id, false))));
	}

	private void bulk(Consumer<BulkRequest> createRequests) {
		var request = new BulkRequest();
		createRequests.accept(request);
		request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		try {
			client.bulk(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	private DeleteRequest deleteRequest(String id, boolean refresh) {
		var request = new DeleteRequest(indexName, id);
		if (refresh) {
			request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return request;
	}

	@Override
	public boolean has(String id) {
		var request = new GetRequest(indexName, id);
		try {
			var response = client.get(request, RequestOptions.DEFAULT);
			if (response == null)
				return false;
			return response.isExists();
		} catch (IOException e) {
			// TODO handle exception
			return false;
		}
	}

	@Override
	public Map<String, Object> get(String id) {
		var request = new GetRequest(indexName, id);
		try {
			var response = client.get(request, RequestOptions.DEFAULT);
			if (response == null)
				return null;
			var source = response.getSource();
			if (source == null || source.isEmpty())
				return null;
			return source;
		} catch (IOException e) {
			// TODO handle exception
			return null;
		}
	}

	@Override
	public List<Map<String, Object>> get(Set<String> ids) {
		var request = new MultiGetRequest();
		ids.forEach(id -> request.add(indexName, id));
		try {
			var response = client.mget(request, RequestOptions.DEFAULT);
			if (response == null)
				return null;
			var results = new ArrayList<Map<String, Object>>();
			var it = response.iterator();
			while (it.hasNext()) {
				var resp = it.next().getResponse();
				if (resp == null)
					continue;
				var source = resp.getSource();
				if (source == null || source.isEmpty())
					continue;
				results.add(source);
			}
			return results;
		} catch (IOException e) {
			// TODO handle exception
			return null;
		}
	}

	@Override
	public void clear() {
		try {
			boolean exists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
			if (!exists)
				return;
			var settings = client.indices()
					.getSettings(new GetSettingsRequest().indices(indexName), RequestOptions.DEFAULT)
					.getIndexToSettings().get(indexName);
			settings = settings.filter(key -> switch (key) {
				case "index.provided_name", "index.creation_date", "index.uuid", "index.version.created" -> false;
				default -> true;
			});
			var mapping = client.indices()
					.getMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT).mappings()
					.get(indexName).getSourceAsMap();
			client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
			var request = new CreateIndexRequest(indexName)
					.settings(settings)
					.mapping(mapping);
			client.indices().create(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

	@Override
	public void delete() {
		try {
			var exists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
			if (!exists)
				return;
			client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO handle exception
		}
	}

}
