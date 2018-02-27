-- After Query Optimizations
USE project3;
SET @v1 = 83745;
SET @v2 = 39409;
SET @v3 = 53014;
SET @v4 = 'crsCode344426';
SET @v5 = 'name874279';
SET @v6 = 'deptId911524';
SET @v7 = 'deptId49933';
SET @v8 = 'deptId776638';


-- 1. List the name of the student with id equal to v1 (id).
-- Same, all ready as good as it gets
SELECT name FROM Student WHERE Student.id = @v1;

-- 2. List the names of students with id in the range of v2 (id) to v3 (inclusive).
-- Same, couldn't find a more efficient way of querying
SELECT name FROM Student WHERE id BETWEEN @v2 AND @v3;

-- 3. List the names of students who have taken course v4 (crsCode).
-- A lot better in terms of rows searched column of Explain
-- added index to crscode to filter search on WHERE clause
CREATE INDEX crs_index on Transcript(CrsCode);
SELECT name FROM Student JOIN Transcript
		ON Transcript.StudId = Student.Id
		WHERE Transcript.CrsCode = @v4;

-- 4. List the names of students who have taken a course taught by professor v5 (name).
-- A lot better in terms of rows searched column of Explain
-- added index to profId to filter search on Professor.Id = Teaching.ProfId
CREATE INDEX prof_index on Teaching(profId);
CREATE INDEX name_index on Professor(name);
SELECT DISTINCT Student.name FROM Student
	JOIN Transcript
		ON Transcript.StudId = Student.Id
	JOIN Teaching
		ON Teaching.CrsCode = Transcript.CrsCode AND Teaching.Semester = Transcript.Semester
	JOIN Professor
		ON Professor.Id = Teaching.ProfId
WHERE Professor.Name = @v5;


-- 5. List the names of students who have taken a course from department v6 (deptId), but not v7.
-- Only gets name as instructed, a few less rows pulled. No real performance inhancement
-- added index to deptID to filter search on Course.deptId = @v6 AND Course.deptId <> @v7
CREATE INDEX dept_index on course(deptId);
SELECT Student.name
FROM Student, Transcript, Course
WHERE Transcript.crsCode= Course.crsCode
AND Student.id = Transcript.studId 
AND Course.deptId = @v6 AND Course.deptId <> @v7 ;

-- 6. List the names of students who have taken all courses offered by department v8 (deptId).
-- Slightly faster in terms of rows searched of Explain
SELECT name FROM Student
JOIN Transcript
	ON Student.id = Transcript.studId
		WHERE crsCode IN
		(SELECT crsCode FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching))
		GROUP BY studId
		HAVING COUNT(*) = 
			(SELECT COUNT(*) FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching));

