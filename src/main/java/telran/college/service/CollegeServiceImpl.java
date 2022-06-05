package telran.college.service;

import java.util.List;

import org.slf4j.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import telran.college.documents.*;
import telran.college.documents.projection.*;
import telran.college.dto.*;
import telran.college.repo.*;

@Service
public class CollegeServiceImpl implements CollegeService {
	
	static Logger LOG = LoggerFactory.getLogger(CollegeServiceImpl.class);
	
	StudentRepository studentsRepository;
	SubjectRepository subjectsRepository;
	
	public CollegeServiceImpl(StudentRepository studentsRepository, SubjectRepository subjectsRepository) {
		this.studentsRepository = studentsRepository;
		this.subjectsRepository = subjectsRepository;
	}

	@Override
	@Transactional
	public void addStudent(Student student) {
		if(studentsRepository.existsById(student.id)) {
			throw new RuntimeException(String.format("student with id %d already exists", student.id));
		}
		StudentDoc studentDoc = new StudentDoc(student.id, student.name);
		studentsRepository.save(studentDoc);
	}

	@Override
	@Transactional
	public void addSubject(Subject subject) {
		if(subjectsRepository.existsById(subject.id)) {
			throw new RuntimeException(String.format("subject with id %d already exists", subject.id));
		}
		SubjectDoc subjectDoc = new SubjectDoc(subject.id, subject.subjectName);
		subjectsRepository.save(subjectDoc);
	}

	@Override
	@Transactional
	public void addMark(Mark mark) {
		StudentDoc studentDoc = studentsRepository.findById(mark.stid).orElse(null);
		if(studentDoc==null) {
			throw new RuntimeException(String.format("student with id %d does not exists", mark.stid));
		}
		SubjectDoc subjectDoc = subjectsRepository.findById(mark.suid).orElse(null);
		if(subjectDoc==null) {
			throw new RuntimeException(String.format("subject with id %d does not exists", mark.suid));
		}
		List<SubjectMark> marks = studentDoc.getMarks(); //get link to marks
		marks.add(new SubjectMark(subjectDoc.getSubjectName(), mark.mark));
		studentsRepository.save(studentDoc);
	}

	@Override
	public List<String> getStudentsSubjectMark(String subjectName, int mark) {
		List<StudentNameProjection> students = studentsRepository.findByMarksSubjectAndMarksMarkGreaterThanEqual(subjectName, mark);
		LOG.debug("students from getStudentsSubjectMark : {}", students);
		return students.stream().map(st -> st.getName()).toList();
	}

	@Override
	public List<Integer> getStudentMarksSubject(String name, String subjectName) {
		StudentMarkProjection marks = studentsRepository.findByNameandMarksSubject(name, subjectName);
		LOG.debug("marks from getStudentMarksSubject : {} of student {}", marks, name);
		return marks.getMarks().stream().filter(sm -> sm.getSubject().equals(subjectName)).map(SubjectMark::getMark).toList();
		//List<SubjectMark> subjects_marks = studentsRepository.findByName(name);
		//return subjects_marks.stream().filter(sm -> sm.getSubject() == subjectName).map(sm -> sm.getMark()).toList();
	}

	@Override
	public List<Student> goodCollegeStudents() {
		return studentsRepository.findGoodStudents();
	}

	@Override
	public List<Student> bestStudents(int nStudents) {
		return studentsRepository.findTopBestStudents(nStudents);
	}

	@Override
	public List<Student> bestStudentsSubject(int nStudents, String subjectName) {
		return studentsRepository.findBestStudentsSubject(nStudents, subjectName);
	}

	@Override
	public Subject subjectGreatestAvgMark() {
		return studentsRepository.findSubjectGreatestAvgMark();
	}

	@Override
	public List<Subject> subjectsAvgMarkGreater(int avgMark) {
		return studentsRepository.findSubjectsAvgMarkGreater(avgMark);
	}

	@Override
	public void deleteStudentsAvgMarkLess(int avgMark) {
		List<Long> studentsID = studentsRepository.findStudentsAvgMarkLess(avgMark);
		LOG.debug("ids AvgMarkLess to delete: {}", studentsID);
		studentsID.stream().forEach(id -> studentsRepository.deleteById(id));
	}

	@Override
	public List<Student> deleteStudentsMarksCountLess(int count) {
		List<Student> students = studentsRepository.findStudentsMarksCountLess(count);
		LOG.debug("students MarksCountLess to delete: {}", students);
		students.stream().forEach(s -> studentsRepository.deleteById(s.id));
		return students;
	}

	@Override
	public List<Student> getStudentsAllMarksSubject(int mark, String subject) {
		return studentsRepository.findStudentsAllMarksSubject(mark, subject);
	}

	@Override
	public List<Student> getStudentsMaxMarksCount() {
		return studentsRepository.findStudentsMaxMarksCount();
	}

	@Override
	public List<Subject> getSubjectsAvgMarkLess(int avgMark) {
		return studentsRepository.findSubjectsAvgMarkLess(avgMark);
	}

}
