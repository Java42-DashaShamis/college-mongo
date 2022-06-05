package telran.college.repo;

import org.springframework.data.mongodb.repository.MongoRepository;

import telran.college.documents.SubjectDoc;
import telran.college.documents.projection.SubjectProjection;

public interface SubjectRepository extends MongoRepository<SubjectDoc, Long> {
	SubjectProjection findBySubjectName(String subjectName);
}
