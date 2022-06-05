package telran.college;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import telran.college.dto.*;
import telran.college.service.CollegeService;

@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StudentsSubjectsMarksMongoApplicationTests {
	@Autowired
	CollegeService collegeService;
@Test
@Order(1)
void addMarks() {
	collegeService.addMark(new Mark(1, 1, 70));
	collegeService.addMark(new Mark(1, 1, 80));
	collegeService.addMark(new Mark(1, 1, 90));
	collegeService.addMark(new Mark(2, 1, 75));
	collegeService.addMark(new Mark(2, 1, 80));
	collegeService.addMark(new Mark(3, 1, 60));
	collegeService.addMark(new Mark(4, 2, 65));
	collegeService.addMark(new Mark(5, 3, 70));
}

@Test
@Order(2)
void getMarksStudentSubjectTest() {
	List<Integer> expected = Arrays.asList(70, 80, 90);
	List<Integer> actual = collegeService.getStudentMarksSubject("student1", "subject1");
	assertIterableEquals(expected, actual);
}
@Test
@Order(3)
void getStudentsSubjectMarks() {
	List<String> expected = Arrays.asList("student1", "student2");
	List<String> actual = collegeService.getStudentsSubjectMark("subject1", 70);
	assertIterableEquals(expected, actual);
}

@Test
@Order(4)
void getGoodStudents() {
	List<Student> expected = Arrays.asList(new Student(1, "student1"), new Student(2, "student2"));
	List<Student> actual = collegeService.goodCollegeStudents();
	assertIterableEquals(expected, actual);
}
@Test
@Order(15)
void deleteStudentsAvgMarkLess() {
	collegeService.deleteStudentsAvgMarkLess(70);
	List<String> actual = collegeService.getStudentsSubjectMark("subject1", 30);
	assertEquals(2, actual.size());
	
}
@Test
@Order(6)
void getSubjectsAvgMarkGreater() {
	List<Subject> expected = Arrays.asList(new Subject(1, "subject1"), new Subject(3, "subject3"));
	List<Subject> actual = collegeService.subjectsAvgMarkGreater(65);
	assertEquals(expected.get(0).id, actual.get(0).id);
	assertIterableEquals(expected, actual);
}
@Test
@Order(16)
void deleteStudentsCountLess() {
	List<Student> expected = Arrays.asList(new Student(5, "student5"));
	List<Student> actual = collegeService.deleteStudentsMarksCountLess(2);
	
	assertIterableEquals(expected, actual);
}
@Test
@Order(5)
void bestStudents() {
	List<Student> expected = Arrays.asList(new Student(1, "student1"));
	List<Student> actual = collegeService.bestStudents(1);
	assertIterableEquals(expected, actual);
}
@Test
@Order (7)
void studentsAllMarksSubjectGreaterEqual () {
	List<Student> expected = Arrays.asList(new Student(2, "student2"));
	List<Student> actual = collegeService.getStudentsAllMarksSubject(75, "subject1");
	assertEquals(expected.size(), actual.size());
	assertIterableEquals(expected, actual);
}
	@Test
	@Order(8)
	void bestStudentsSubject() {
		List<Student> expected = Arrays.asList(new Student(1, "student1"));
		List<Student> actual = collegeService.bestStudentsSubject(1, "subject1");
		assertIterableEquals(expected, actual);

	}
	@Test
	@Order(9)
	void subjectWithGreatestAvgMark() {
		Subject expected = new Subject(1, "subject1");
		Subject actual = collegeService.subjectGreatestAvgMark();
		assertEquals(expected, actual);

	}
	@Test
	@Order(11)
	void studentsMaxMarksCount() {
		List<Student> expected = Arrays.asList(new Student(1, "student1"));
		List<Student> actual = collegeService.getStudentsMaxMarksCount();
		assertIterableEquals(expected, actual);

	}
	@Test
	@Order(10)
	void subjectsAvgMarkLess() {
		List<Subject> expected = Arrays.asList(new Subject(2, "subject2"));
		List<Subject> actual = collegeService.getSubjectsAvgMarkLess(70);
		assertIterableEquals(expected, actual);

	}

}
