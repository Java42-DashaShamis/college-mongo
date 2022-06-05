package telran.college.dto;

import java.util.Objects;

public class Subject {
public long id;
public String subjectName;
public Subject(long id, String subjectName) {
	this.id = id;
	this.subjectName = subjectName;
}
public Subject() {
}
@Override
public int hashCode() {
	return Objects.hash(id, subjectName);
}
@Override
public boolean equals(Object obj) {
	if (this == obj)
		return true;
	if (obj == null)
		return false;
	if (getClass() != obj.getClass())
		return false;
	Subject other = (Subject) obj;
	return id == other.id && Objects.equals(subjectName, other.subjectName);
}
}
