package telran.college.repo;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.aggregation.BooleanOperators.Or;
import org.springframework.data.mongodb.core.query.Criteria;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import org.springframework.stereotype.Repository;

import telran.college.documents.StudentDoc;
import telran.college.documents.projection.SubjectProjection;
import telran.college.dto.Student;
import telran.college.dto.Subject;

@Repository
public class StudentAggregationRepositoryImpl implements StudentAggregationRepository {
	private static final String AVG_MARK_FIELD = "avgMark";
	private static final String COUNT_MARK_FIELD = "countOfMarks";
	private static final String MIN_MARK = "minMark";
	@Autowired
	MongoTemplate mongoTemplate;
	@Autowired
	SubjectRepository subjectRepository;
	
	@Override
	public List<Student> findTopBestStudents(int nStudents) {
		UnwindOperation unwindOperation = unwind("marks");
		GroupOperation groupOperation = group("id","name").avg("marks.mark").as(AVG_MARK_FIELD); //collection with field "_d" as document with fields id and name and field avgMark
		SortOperation sortOperation = sort(Direction.DESC, AVG_MARK_FIELD);
		LimitOperation limitOperation = limit(nStudents);
		ProjectionOperation projectionOperation = project().andExclude(AVG_MARK_FIELD);
		Aggregation aggregation = newAggregation(unwindOperation, groupOperation, sortOperation, limitOperation, projectionOperation);
		var documents = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class);
		return getStudentResult(documents);
	}
	private List<Student> getStudentResult(AggregationResults<Document> documents) {
		return documents.getMappedResults().stream().map(this::getStudent).sorted((s1,s2) -> (int)(s1.id - s2.id)).toList();
	}
	private Student getStudent(Document doc) {
		Document idDocument = doc.get("_id", Document.class);
		return new Student(idDocument.getLong("id"), idDocument.getString("name"));
	}
	
	@Override
	public List<Student> findGoodStudents() {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(group("id", "name").avg("marks.mark").as(AVG_MARK_FIELD));
		double avgMark = getCollegeAvgMark();
		pipeline.add(match(Criteria.where(AVG_MARK_FIELD).gt(avgMark)));
		pipeline.add(project().andExclude(AVG_MARK_FIELD));
		Aggregation aggregation = newAggregation(pipeline);
		var documents = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class);
		return getStudentResult(documents);
	}
	private double getCollegeAvgMark() {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(group().avg("marks.mark").as(AVG_MARK_FIELD));
		Aggregation aggregation = newAggregation(pipeline);
		var document = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class).getUniqueMappedResult();
		return document.getDouble(AVG_MARK_FIELD);
	}
	
	@Override
	public List<Student> findBestStudentsSubject(int nStudents, String subjectName) {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(match(Criteria.where("marks.subject").is(subjectName)));
		pipeline.add(group("id", "name").avg("marks.mark").as(AVG_MARK_FIELD));
		pipeline.add(sort(Direction.DESC, AVG_MARK_FIELD));
		pipeline.add(limit(nStudents));
		pipeline.add(project().andExclude(AVG_MARK_FIELD));
		Aggregation aggregation = newAggregation(pipeline);
		var documents = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class);
		return getStudentResult(documents);
	}
	
	@Override
	public Subject findSubjectGreatestAvgMark() {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(group("marks.subject").avg("marks.mark").as(AVG_MARK_FIELD));
		pipeline.add(sort(Direction.DESC, AVG_MARK_FIELD));
		pipeline.add(limit(1));
		pipeline.add(project().andExclude(AVG_MARK_FIELD));
		Aggregation aggregation = newAggregation(pipeline);
		var document = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class).getUniqueMappedResult();
		return getSubject(document);
	}
	private List<Subject> getSubjectResult(AggregationResults<Document> documents) {
		return documents.getMappedResults().stream().map(this::getSubject).sorted((s1,s2) -> (int)(s1.id - s2.id)).toList();
	}
	private Subject getSubject(Document document) {
		String subjectName = document.get("_id", String.class);
		SubjectProjection subproj = subjectRepository.findBySubjectName(subjectName);
		return new Subject(subproj.getId(), subproj.getSubjectName());
	}
	
	@Override
	public List<Subject> findSubjectsAvgMarkGreater(int avgMark) {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(group("marks.subject").avg("marks.mark").as(AVG_MARK_FIELD));
		pipeline.add(match(Criteria.where(AVG_MARK_FIELD).gt(avgMark)));
		pipeline.add(project().andExclude(AVG_MARK_FIELD));
		Aggregation aggregation = newAggregation(pipeline);
		var documents = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class);
		return getSubjectResult(documents);
	}
	
	@Override
	public List<Long> findStudentsAvgMarkLess(int avgMark) {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(group("id").avg("marks.mark").as(AVG_MARK_FIELD));
		pipeline.add(match(new Criteria().orOperator(Criteria.where(AVG_MARK_FIELD).lt(avgMark), Criteria.where(AVG_MARK_FIELD).isNull())));
		pipeline.add(project().andExclude(AVG_MARK_FIELD));
		Aggregation aggregation = newAggregation(pipeline);
		var documents = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class);
		return documents.getMappedResults().stream().map(idp -> idp.get("_id", Long.class)).toList();
	}
	
	@Override
	public List<Student> findStudentsMarksCountLess(int count) {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(group("id", "name").count().as(COUNT_MARK_FIELD));
		pipeline.add(match(new Criteria().orOperator(Criteria.where(COUNT_MARK_FIELD).lt(count), Criteria.where(COUNT_MARK_FIELD).isNull())));
		pipeline.add(project().andExclude(COUNT_MARK_FIELD));
		Aggregation aggregation = newAggregation(pipeline);
		var documents = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class);
		return getStudentResult(documents);
	}
	
	@Override
	public List<Student> findStudentsAllMarksSubject(int mark, String subject) {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(match(Criteria.where("marks.subject").is(subject)));
		pipeline.add(group("id","name").min("marks.mark").as(MIN_MARK));
		pipeline.add(match(Criteria.where(MIN_MARK).gte(mark)));
		pipeline.add(project().andExclude("marks"));
		Aggregation aggregation = newAggregation(pipeline);
		var documents = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class);
		return getStudentResult(documents);
	}
	
	@Override
	public List<Student> findStudentsMaxMarksCount() {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(group("id", "name").count().as(COUNT_MARK_FIELD));
		pipeline.add(match(Criteria.where(COUNT_MARK_FIELD).is(findMaxMarksCount())));
		pipeline.add(project().andExclude(COUNT_MARK_FIELD));
		Aggregation aggregation = newAggregation(pipeline);
		var documents = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class);
		return getStudentResult(documents);
	}
	private int findMaxMarksCount() {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(group("id", "name").count().as(COUNT_MARK_FIELD));
		pipeline.add(sort(Direction.DESC, COUNT_MARK_FIELD));
		pipeline.add(limit(1));
		pipeline.add(project().andExclude("id","name"));
		Aggregation aggregation = newAggregation(pipeline);
		var document = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class).getUniqueMappedResult();
		return document.getInteger(COUNT_MARK_FIELD);
	}
	
	@Override
	public List<Subject> findSubjectsAvgMarkLess(int avgMark) {
		ArrayList<AggregationOperation> pipeline = new ArrayList<>();
		pipeline.add(unwind("marks"));
		pipeline.add(group("marks.subject").avg("marks.mark").as(AVG_MARK_FIELD));
		pipeline.add(match(new Criteria().orOperator(Criteria.where(AVG_MARK_FIELD).lt(avgMark), Criteria.where(AVG_MARK_FIELD).isNull())));
		Aggregation aggregation = newAggregation(pipeline);
		var documents = mongoTemplate.aggregate(aggregation, StudentDoc.class, Document.class);
		return getSubjectResult(documents);
	}
	
	
	
}
