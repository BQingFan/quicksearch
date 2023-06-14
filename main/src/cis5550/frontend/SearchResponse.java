package cis5550.frontend;

import java.util.ArrayList;
import java.util.List;

public class SearchResponse {
	public List<String> results = new ArrayList<>();
	public SearchResponse(List<String> resultsRaw) {
		this.results = resultsRaw;
	}
}
