package telran.college.repo;

import java.util.List;

import telran.college.dto.Student;
import telran.college.dto.Subject;

public interface StudentAggregationRepository {
	List<Student> findTopBestStudents(int nStudents);
	List<Student> findGoodStudents();
	List<Student> findBestStudentsSubject(int nStudents, String subjectName);
	Subject findSubjectGreatestAvgMark();
	List<Subject> findSubjectsAvgMarkGreater(int avgMark);
	List<Long> findStudentsAvgMarkLess(int avgMark);
	List<Student> findStudentsMarksCountLess(int count);
	List<Student> findStudentsAllMarksSubject(int mark, String subject);
	List<Student> findStudentsMaxMarksCount();
	List<Subject> findSubjectsAvgMarkLess(int avgMark);
}
