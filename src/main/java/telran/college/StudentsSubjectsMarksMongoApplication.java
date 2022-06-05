package telran.college;

import java.util.concurrent.ThreadLocalRandom;

import java.util.stream.IntStream;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import telran.college.dto.*;
import telran.college.service.CollegeService;

@SpringBootApplication
public class StudentsSubjectsMarksMongoApplication {

	
		private static final int MIN_MARK = 60;
		private static final int MAX_MARK = 100;
		@Value("${app.amount.students}")
		int nStudents;
		@Value("${app.amount.subjects}")
		int nSubjects;
		@Value("${app.amount.marks}")
		int nMarks;
		/**********************/
		@Autowired
		CollegeService collegeService;
		static ThreadLocalRandom rand = ThreadLocalRandom.current();

		public static void main(String[] args) {
			ConfigurableApplicationContext ctx =
					SpringApplication.run(StudentsSubjectsMarksMongoApplication.class, args);
			
			
		}
		@PostConstruct
		void createDb() {
			createStudents();
			createSubjects();
			createMarks();
			
			
		}
		private void createMarks() {
			IntStream.range(0, nMarks).forEach(i -> collegeService.addMark(getRandomMark()));
			
		}
		private void createSubjects() {
			IntStream.rangeClosed(1, nSubjects)
			.forEach(i -> collegeService.addSubject(new Subject(i, "subject" + i)));
			
		}
		void createStudents() {
			IntStream.rangeClosed(1, nStudents)
			.forEach(i -> collegeService.addStudent(new Student(i, "student" + i)));
		}
		Mark getRandomMark() {
			long suid = rand.nextLong(1, nSubjects + 1 );
			long stid = rand.nextLong(1, nStudents + 1 );
			int mark = rand.nextInt(MIN_MARK, MAX_MARK + 1 );
			return new Mark(stid, suid, mark);
		}
	

}
