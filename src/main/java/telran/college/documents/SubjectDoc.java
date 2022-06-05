package telran.college.documents;

import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "subjects")
public class SubjectDoc {
	long id;
	String subjectName;
	public SubjectDoc(long id, String subjectName) {
		this.id = id;
		this.subjectName = subjectName;
	}
	public long getId() {
		return id;
	}
	public String getSubjectName() {
		return subjectName;
	}
	@Override
	public String toString() {
		return "SubjectDoc [id=" + id + ", subjectName=" + subjectName + "]";
	}
	
}
