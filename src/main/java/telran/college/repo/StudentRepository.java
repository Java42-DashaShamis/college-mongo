package telran.college.repo;

import java.util.List;


import org.springframework.data.mongodb.repository.*;

import telran.college.documents.StudentDoc;
import telran.college.documents.projection.StudentMarkProjection;
import telran.college.documents.projection.StudentNameProjection;

public interface StudentRepository extends MongoRepository<StudentDoc, Long>, StudentAggregationRepository {
	
	@Query(value="{'marks.subject' : ?0, 'marks.mark' : {$gte:?1}}", fields = "{name : 1}")
	List<StudentNameProjection> findByMarksSubjectAndMarksMarkGreaterThanEqual(String subjectName, int mark);
	
	@Query(value = "{name : ?0, filter{'marks.subject' : ?1}}", fields = "{'marks' : 1}")
	StudentMarkProjection findByNameandMarksSubject(String name, String subjectName);
}
