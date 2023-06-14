package cis5550.frontend;

public class Result {
	
	String url;
	String title;
	String description;
	
	public Result(String url, String title, String description) {
		this.url = url;
		this.title = title;
		this.description = description;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public void setDescription(String description) {
		this.description = description;
	}
	
	public String getTitle() {
		return this.title;
	}
	
	public String getDescription() {
		return this.description;
	}
}