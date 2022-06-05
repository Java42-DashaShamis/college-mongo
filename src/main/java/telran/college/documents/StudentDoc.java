package telran.college.documents;

import java.util.*;

import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "students")
public class StudentDoc {
	long id;
	String name;
	List<SubjectMark> marks = new ArrayList<>();
	public StudentDoc(long id, String name) {
		this.id = id;
		this.name = name;
	}
	public long getId() {
		return id;
	}
	public String getName() {
		return name;
	}
	public List<SubjectMark> getMarks() {
		return marks;
	}
	@Override
	public String toString() {
		return "StudentDoc [id=" + id + ", name=" + name + ", marks=" + marks + "]";
	}
	
}
