package cis5550.frontend;

import java.util.ArrayList;
import java.util.List;

public class Suggestions {
	public List<String> suggestions = new ArrayList<>();
	public Suggestions (List<String> suggestionsRaw) {
		this.suggestions = suggestionsRaw;
	}
}
