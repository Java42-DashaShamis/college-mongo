package telran.college.service;

import java.util.List;

import telran.college.dto.*;

public interface CollegeService {
	void addStudent(Student student);
	void addSubject(Subject subject);
	void addMark(Mark mark);
	List<String> getStudentsSubjectMark(String subjectName, int mark); //get student names those who have at least one mark 
	// greater or equal the given one
	List<Integer> getStudentMarksSubject(String name, String subjectName);
	List<Student> goodCollegeStudents();//students with avg mark greater than total avg mark of the college
	List<Student> bestStudents(int nStudents); //the given number of the best students
	List<Student> bestStudentsSubject(int nStudents, String subjectName);
	Subject subjectGreatestAvgMark();
	List<Subject> subjectsAvgMarkGreater(int avgMark);
	void deleteStudentsAvgMarkLess(int avgMark);//remove all students having avg mark less than the given one
	List<Student> deleteStudentsMarksCountLess(int count);//remove all students having amount
	//of marks less than the given one; returns being deleted students
	List<Student> getStudentsAllMarksSubject(int mark, String subject);//getting students having all marks on given subject >= mark
	List<Student> getStudentsMaxMarksCount(); //getting students having maximal count of marks
	List<Subject> getSubjectsAvgMarkLess(int avgMark); //getting subjects having avg mark less than the given (using right join for subjects not having marks at all)
}
