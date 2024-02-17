CREATE TABLE department (
    dept_id CHAR(3) PRIMARY KEY NOT NULL,
    dept_name VARCHAR(40) NOT NULL UNIQUE
);


CREATE TABLE student (
    student_id CHAR(11) PRIMARY KEY NOT NULL,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40),
    address VARCHAR(100),
    contact_number CHAR(10) NOT NULL UNIQUE,
    email_id VARCHAR(50) UNIQUE,
    tot_credits INTEGER NOT NULL CHECK (tot_credits >= 0),
    dept_id CHAR(3) REFERENCES department(dept_id)
);

CREATE TABLE courses (
    course_id CHAR(6) PRIMARY KEY NOT NULL,
    course_name VARCHAR(20) NOT NULL UNIQUE,
    course_desc TEXT,
    credits NUMERIC NOT NULL CHECK (credits > 0),
    dept_id CHAR(3) REFERENCES department(dept_id),
    CONSTRAINT check_course_id_format 
        CHECK (
            SUBSTRING(course_id FROM 1 FOR 3) = dept_id 
            AND SUBSTRING(course_id FROM 4 FOR 3) ~ '^\d{3}$'
        )

);

CREATE  TABLE student_courses (
    student_id CHAR(11)  REFERENCES student(student_id),
    course_id CHAR(6)  REFERENCES courses(course_id),
    session VARCHAR(9) NOT NULL,
    semester INTEGER NOT NULL CHECK (semester IN (1, 2)),
    grade NUMERIC NOT NULL CHECK (grade >= 0 AND grade <= 10),
    PRIMARY KEY (student_id, course_id)
    -- FOREIGN KEY (course_id, session, semester) REFERENCES course_offers(course_id, session, semester)
);

CREATE TABLE professor (
    professor_id VARCHAR(10) PRIMARY KEY NOT NULL,
    professor_first_name VARCHAR(40) NOT NULL,
    professor_last_name VARCHAR(40) NOT NULL,
    office_number VARCHAR(20),
    contact_number CHAR(10) NOT NULL,
    start_year INTEGER NOT NULL,
    resign_year INTEGER NOT NULL CHECK (resign_year >= start_year),
    dept_id CHAR(3) NOT NULL REFERENCES department(dept_id)
);


CREATE  TABLE course_offers (
    course_id CHAR(6) NOT NULL REFERENCES courses(course_id),
    session VARCHAR(9) NOT NULL,
    semester INTEGER NOT NULL CHECK (semester IN (1, 2)),
    professor_id VARCHAR(10) NOT NULL REFERENCES professor(professor_id),
    capacity INTEGER,
    enrollments INTEGER,
    PRIMARY KEY (course_id, session, semester)
);


CREATE TABLE valid_entry (
    dept_id CHAR(3) NOT NULL REFERENCES department(dept_id),
    entry_year INTEGER NOT NULL,
    seq_number INTEGER NOT NULL,
    PRIMARY KEY (dept_id, entry_year)
);


-- 2.1.1, 2.1.3
-- validate student id, validate email_id
CREATE OR REPLACE FUNCTION validate_student_id_function() RETURNS TRIGGER AS $$
DECLARE
    -- local variables 
    dept_seq_number INTEGER;
    new_entry_year INTEGER; 
    new_dept_id CHAR(3); 
    seq_number INTEGER; 
    new_email_id VARCHAR(50); 
    new_student_id VARCHAR(11); 
    email_domain VARCHAR(50);
BEGIN
    new_entry_year := LEFT(NEW.student_id, 4)::INTEGER;
    new_dept_id := SUBSTRING(NEW.student_id FROM 5 FOR 3);
    seq_number := RIGHT(NEW.student_id, 3)::INTEGER;

    -- Check if entry year and dept id are valid in valid_entry table
    IF NOT EXISTS (
        SELECT * FROM valid_entry 
        WHERE valid_entry.dept_id = NEW.dept_id  AND valid_entry.entry_year = new_entry_year
    ) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    SELECT valid_entry.seq_number INTO dept_seq_number 
    FROM valid_entry 
    WHERE valid_entry.dept_id = new_dept_id 
    AND valid_entry.entry_year = new_entry_year;

    -- Validate Sequence Number 
    -- if not validated then raise 'invalid' exception
    IF dept_seq_number <> seq_number THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    -- now we check email id
    -- email_id should contain '@' separator
    IF POSITION('@' IN NEW.email_id) = 0 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    email_domain := SUBSTRING(NEW.email_id FROM POSITION('@' IN NEW.email_id) + 1);
    new_student_id := LEFT(NEW.email_id, 10);

    -- valid email address has the following parts: 
    -- 1) domain ==  dept_id + .iitd.ac.in
    -- 2) new_student_id == NEW.student_id 

    -- Check if email domain matches dept_id + 'iitd.ac.in' and student_id components match
    IF email_domain <> new_dept_id || '.iitd.ac.in' OR new_student_id <> NEW.student_id THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_student_id
BEFORE INSERT ON student
FOR EACH ROW
EXECUTE FUNCTION validate_student_id_function();


-- -- 2.1.2
CREATE OR REPLACE FUNCTION update_seq_number_function() 
RETURNS TRIGGER AS $$
DECLARE
    new_entry_year INTEGER; 
BEGIN

    -- fetch the new entry details 
    new_entry_year := LEFT(NEW.student_id, 4)::INTEGER;

    
    UPDATE valid_entry
    SET seq_number = seq_number + 1
    WHERE valid_entry.dept_id = NEW.dept_id AND valid_entry.entry_year = new_entry_year;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_seq_number
AFTER INSERT ON student
FOR EACH ROW
EXECUTE FUNCTION update_seq_number_function();


-- -- 2.1.4

CREATE TABLE student_dept_change (
    old_student_id CHAR(11) NOT NULL,
    old_dept_id CHAR(3) NOT NULL,
    new_dept_id CHAR(3) NOT NULL,
    new_student_id CHAR(11) NOT NULL,
    PRIMARY KEY (old_student_id, new_student_id),
    FOREIGN KEY (old_dept_id) REFERENCES department(dept_id),
    FOREIGN KEY (new_dept_id) REFERENCES department(dept_id)
);


CREATE OR REPLACE FUNCTION log_student_dept_change_function() RETURNS TRIGGER AS $$
DECLARE
    old_entry_year INTEGER;
    new_entry_year INTEGER;
    old_dept_id CHAR(3); 
    new_dept_id CHAR(3); 
    avg_grade NUMERIC;
BEGIN

    old_entry_year := LEFT(OLD.student_id, 4)::INTEGER;
    new_entry_year := LEFT(NEW.student_id, 4)::INTEGER;
    old_dept_id := SUBSTRING(OLD.student_id FROM 5 FOR 3);
    new_dept_id := SUBSTRING(NEW.student_id FROM 5 FOR 3);

    -- if department changed before
    IF OLD.dept_id <> NEW.dept_id AND EXISTS (
        SELECT * 
        FROM student_dept_change s
        WHERE s.old_student_id = OLD.student_id 
    ) THEN
        RAISE EXCEPTION 'Department can be changed only once';
    END IF;

    -- if department is not changed then simply update whatever is done
    IF OLD.dept_id = NEW.dept_id THEN 
        RETURN NEW; 
    END IF; 

    -- entry year should be >= 2022
    IF old_entry_year < 2022 THEN
        RAISE EXCEPTION 'Entry year must be >= 2022';
    END IF;

    -- Check average grade constraint
    SELECT AVG(grade) INTO avg_grade
    FROM student_courses
    WHERE student_id = OLD.student_id;

    IF avg_grade IS NULL OR avg_grade <= 8.5 THEN
        RAISE EXCEPTION 'Low Grade';
    END IF;

    -- Student Row update 
    UPDATE student
    SET student_id = NEW.student_id
    WHERE student_id = OLD.student_id;

    -- Add to student_dept_change table
    INSERT INTO student_dept_change (old_student_id, old_dept_id, new_dept_id, new_student_id)
    VALUES (OLD.student_id, OLD.dept_id, NEW.dept_id, NEW.student_id);

    -- Update valid entry table
    UPDATE valid_entry
    SET seq_number = seq_number + 1
    WHERE dept_id = NEW.dept_id and entry_year = new_entry_year;

    -- Update corresponding valid email id in student table
    UPDATE student
    SET email_id = NEW.student_id || '@' || new_dept_id || '.iitd.ac.in'
    WHERE student_id = NEW.student_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger to associate with the student table
CREATE TRIGGER log_student_dept_change
BEFORE UPDATE ON student
FOR EACH ROW
EXECUTE FUNCTION log_student_dept_change_function();


-- 2.2.1
-- course_eval VIEW
CREATE OR REPLACE VIEW course_eval AS
SELECT
    sc.course_id,
    sc.session,
    sc.semester,
    COUNT(distinct sc.student_id) AS number_of_students,
    AVG(sc.grade) AS average_grade,
    MAX(sc.grade) AS max_grade,
    MIN(sc.grade) AS min_grade
FROM
    student_courses sc
GROUP BY
    sc.course_id,
    sc.session,
    sc.semester;

-- course_eval update function
CREATE OR REPLACE FUNCTION update_course_eval()
RETURNS TRIGGER AS $$
BEGIN
    -- REFRESH MATERIALIZED VIEW course_eval;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER course_eval_update_trigger
AFTER INSERT OR UPDATE ON student_courses
FOR EACH STATEMENT
EXECUTE FUNCTION update_course_eval();


-- -- 2.2.2

CREATE OR REPLACE FUNCTION update_student_tot_credits()
RETURNS TRIGGER AS $$
DECLARE
    total_credits INTEGER;
    new_student_id VARCHAR(11); 
BEGIN

    new_student_id := NEW.student_id; 

    SELECT SUM(c.credits) INTO total_credits
    FROM student_courses sc
    JOIN courses c 
    ON sc.course_id = c.course_id
    WHERE student_id = NEW.student_id ;

    UPDATE student
    SET tot_credits = total_credits
    WHERE student_id = NEW.student_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_student_tot_credits_trigger
AFTER INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_student_tot_credits();

-- -- 2.2.3
CREATE OR REPLACE FUNCTION check_course_enrollment_limit()
RETURNS TRIGGER AS $$
DECLARE
    course_count INTEGER;
    student_credits INTEGER;
    course_credits INTEGER; 

BEGIN

    SELECT COUNT(distinct course_id) INTO course_count
    FROM student_courses
    WHERE student_id = NEW.student_id AND session = NEW.session AND semester = NEW.semester;

    IF course_count >= 5 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    SELECT tot_credits
    INTO student_credits
    FROM student
    WHERE student_id = NEW.student_id;

    SELECT credits into course_credits
    FROM courses 
    WHERE course_id = NEW.course_id; 

    IF student_credits + course_credits > 60 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_enrollment_limit_trigger
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_enrollment_limit();

-- -- 2.2.4
CREATE OR REPLACE FUNCTION check_course_first_year()
RETURNS TRIGGER AS $$
DECLARE
    student_first_year INTEGER;
    session_year INTEGER; 
    course_credits INTEGER; 
BEGIN
    -- Get the first year of the student from their student ID
    student_first_year := CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER);
    session_year := CAST(SUBSTRING(NEW.session FROM 1 FOR 4) AS INTEGER); 

    SELECT credits INTO course_credits
    FROM courses 
    WHERE courses.course_id = NEW.course_id; 

    IF course_credits = 5 AND student_first_year <> session_year THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_first_year_trigger
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_first_year();


-- -- 2.2.5

CREATE OR REPLACE VIEW student_semester_summary AS
SELECT
    sc.student_id,
    sc.session,
    sc.semester,
    SUM(sc.grade * c.credits) / SUM(c.credits) AS sgpa,
    SUM(c.credits) AS credits
FROM
    student_courses sc
JOIN
    courses c ON sc.course_id = c.course_id
WHERE 
    sc.grade >= 5
GROUP BY
    sc.student_id, sc.session, sc.semester;

CREATE OR REPLACE FUNCTION update_student_semester_summary()
RETURNS TRIGGER AS $$
DECLARE
    semester_credits INTEGER;
    course_credits INTEGER; 
    old_course_credits INTEGER; 
BEGIN

    SELECT credits INTO course_credits
    FROM courses 
    WHERE courses.course_id = NEW.course_id; 

    SELECT credits INTO old_course_credits
    FROM courses 
    WHERE courses.course_id = OLD.course_id; 

    IF TG_OP = 'INSERT' THEN
        -- credits in the current semester 
        SELECT SUM(c.credits)
        INTO semester_credits
        FROM student_courses sc
        JOIN courses c ON sc.course_id = c.course_id
        WHERE sc.student_id = NEW.student_id
        AND sc.session = NEW.session
        AND sc.semester = NEW.semester;

        IF semester_credits + course_credits > 26 THEN
            RAISE EXCEPTION 'invalid';
        END IF;

        UPDATE student
        SET tot_credits = tot_credits + course_credits
        WHERE student_id = NEW.student_id;

    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE student
        SET tot_credits = tot_credits - old_course_credits + course_credits
        WHERE student_id = NEW.student_id;

    ELSIF TG_OP = 'DELETE' THEN
        UPDATE student
        SET tot_credits = tot_credits - old_course_credits
        WHERE student_id = OLD.student_id;

    END IF;

    -- Update student semester summary view
    -- REFRESH MATERIALIZED VIEW student_semester_summary;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- INSERT TRIGGER
CREATE TRIGGER update_student_semester_summary_insert_trigger
AFTER INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_student_semester_summary();

-- UPDATE TRIGGER
CREATE TRIGGER update_student_semester_summary_update_trigger
AFTER UPDATE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_student_semester_summary();

-- DELETE TRIGGER 
CREATE TRIGGER update_student_semester_summary_delete_trigger
AFTER DELETE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_student_semester_summary();


-- -- 2.2.6
CREATE OR REPLACE FUNCTION check_course_capacity()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the course capacity is full
    IF (SELECT enrollments >= capacity FROM course_offers WHERE course_id = NEW.course_id AND session = NEW.session AND semester = NEW.semester) THEN
        RAISE EXCEPTION 'course is full';
    ELSE
        -- Update the number of enrollments in course_offers
        UPDATE course_offers 
        SET enrollments = enrollments + 1 
        WHERE course_id = NEW.course_id AND session = NEW.session AND semester = NEW.semester;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_capacity_trigger
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_capacity();


-- -- 2.3.1
CREATE OR REPLACE FUNCTION remove_course_entries()
RETURNS TRIGGER AS $$
DECLARE 
    course_credits INTEGER; 
BEGIN


    SELECT credits into course_credits
    FROM courses 
    WHERE course_id = OLD.course_id; 

    -- clear from student_courses
    DELETE FROM student_courses
    WHERE course_id = OLD.course_id
    AND session = OLD.session
    AND semester = OLD.semester;

    -- update tot_credits in student
    UPDATE student
    SET tot_credits = tot_credits - (
        SELECT SUM(credits)
        FROM student_courses sc
        JOIN courses c ON sc.course_id = c.course_id
        WHERE sc.student_id = student.student_id
        AND sc.session = OLD.session
        AND sc.semester = OLD.semester
        AND sc.course_id = OLD.course_id
    )
    WHERE student_id IN (
        SELECT student_id
        FROM student_courses
        WHERE course_id = OLD.course_id
        AND session = OLD.session
        AND semester = OLD.semester
    );

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION add_course_validation()
RETURNS TRIGGER AS $$
BEGIN
    -- check if course_id exists
    IF NOT EXISTS (SELECT 1 FROM courses WHERE course_id = NEW.course_id) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    -- Check if professor_id exists 
    IF NOT EXISTS (SELECT 1 FROM professor WHERE professor_id = NEW.professor_id) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- DELETE TRIGGER
CREATE TRIGGER remove_course_entries_trigger
AFTER DELETE ON course_offers
FOR EACH ROW
EXECUTE FUNCTION remove_course_entries();


-- ADDITION TRIGGER
CREATE TRIGGER add_course_validation_trigger
BEFORE INSERT ON course_offers
FOR EACH ROW
EXECUTE FUNCTION add_course_validation();


-- 2.3.2
CREATE OR REPLACE FUNCTION check_course_offers_entry()
RETURNS TRIGGER AS $$
DECLARE
    start_year INTEGER;
BEGIN
    -- Check if the professor is already teaching 4 courses in the session
    IF (SELECT COUNT(*) >= 4 
        FROM course_offers 
        WHERE professor_id = NEW.professor_id 
        AND session = NEW.session) THEN
        RAISE EXCEPTION 'invalid';
    END IF;


    start_year := CAST(SUBSTRING(NEW.session FROM 1 FOR 4) AS INTEGER); 
    -- Check if the course is being offered before the associated professor resigns
    IF start_year >= (SELECT resign_year FROM professor WHERE professor_id = NEW.professor_id) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_offers_entry_trigger
BEFORE INSERT ON course_offers
FOR EACH ROW
EXECUTE FUNCTION check_course_offers_entry();



-- -- 2.4.1
CREATE OR REPLACE FUNCTION update_department_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.dept_id <> OLD.dept_id THEN
        INSERT INTO department (dept_id, dept_name)
        VALUES (NEW.dept_id, 'DUMMY');
        
        UPDATE professor
        SET dept_id = NEW.dept_id
        WHERE dept_id = OLD.dept_id;

        ALTER TABLE student DISABLE TRIGGER ALL;
        ALTER TABLE course_offers DISABLE TRIGGER ALL;
        ALTER TABLE student_courses DISABLE TRIGGER ALL;
        ALTER TABLE department DISABLE TRIGGER ALL;

        UPDATE student
        SET dept_id = NEW.dept_id
        WHERE dept_id = OLD.dept_id;
        


        UPDATE courses
        SET course_id = NEW.dept_id || SUBSTRING(course_id FROM 4)
        WHERE course_id LIKE OLD.dept_id || '%';


        
        UPDATE course_offers
        SET course_id = NEW.dept_id || SUBSTRING(course_id FROM 4)
        WHERE course_id LIKE OLD.dept_id || '%';
        

        
        UPDATE student_courses
        SET course_id = NEW.dept_id || SUBSTRING(course_id FROM 4)
        WHERE course_id LIKE OLD.dept_id || '%';
        


        DELETE FROM department 
        WHERE dept_id = NEW.dept_id; 

        ALTER TABLE student ENABLE TRIGGER ALL;
        ALTER TABLE department ENABLE TRIGGER ALL;
        ALTER TABLE course_offers ENABLE TRIGGER ALL;
        ALTER TABLE student_courses ENABLE TRIGGER ALL;


    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_department_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM student WHERE dept_id = OLD.dept_id) THEN
        RAISE EXCEPTION 'Department has students';
    ELSE
        -- DELETE PROFS
        DELETE FROM professor WHERE dept_id = OLD.dept_id;
        -- DELETE COURSES
        DELETE FROM courses WHERE course_id LIKE OLD.dept_id || '%';
        DELETE FROM course_offers WHERE course_id LIKE OLD.dept_id || '%';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_department_trigger
AFTER UPDATE ON department
FOR EACH ROW
EXECUTE FUNCTION update_department_trigger();

CREATE TRIGGER delete_department_trigger
BEFORE DELETE ON department
FOR EACH ROW
EXECUTE FUNCTION delete_department_trigger();
