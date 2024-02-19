from conf.models import Teacher, Group, Student, Subject, Grade
from typing import Union, Optional
from customlogger import logger
from datetime import datetime
from conf.db import session
import argparse


def create_teacher(name: str) -> Optional[int]:
    """
    Creates a new teacher in the database.

    Args:
        name (str): Full name of the teacher.

    Returns:
        Optional[int]: Identifier of the created teacher in the database, or None if an error occurred.
    """
    try:
        teacher = Teacher(fullname=name)
        session.add(teacher)
        session.flush()
        teacher_id = teacher.id
        session.commit()
        logger.log(f"New teacher {name} added successfully!")
        return teacher_id
    except Exception as e:
        session.rollback()
        logger.log(f"An error occurred while creating teacher: {e}", level=40)
        return None
    finally:
        session.close()


def create_group(name: str) -> Optional[int]:
    """
    Creates a new group in the database.

    Args:
        name (str): Name of the group.

    Returns:
        Optional[int]: Identifier of the created group in the database, or None if an error occurred.
    """
    try:
        group = Group(name=name)
        session.add(group)
        session.flush()
        group_id = group.id
        session.commit()
        logger.log(f"New group {name} created successfully!")
        return group_id
    except Exception as e:
        session.rollback()
        logger.log(f"An error occurred while creating group: {e}", level=40)
        return None
    finally:
        session.close()


def create_student(name: str, group_id: int) -> Optional[int]:
    """
    Creates a new student in the database.

    Args:
        name (str): Full name of the student.
        group_id (int): Identifier of the group to which the student belongs.

    Returns:
        Optional[int]: Identifier of the created student in the database, or None if an error occurred.
    """
    try:
        student = Student(fullname=name, group_id=group_id)
        session.add(student)
        session.flush()
        student_id = student.id
        session.commit()
        logger.log(f"New student {name} added successfully!")
        return student_id
    except Exception as e:
        session.rollback()
        logger.log(f"An error occurred while creating student: {e}", level=40)
        return None
    finally:
        session.close()


def create_subject(name: str, teacher_id: int) -> Optional[int]:
    """
    Creates a new subject in the database.

    Args:
        name (str): Name of the subject.
        teacher_id (int): Identifier of the teacher who teaches this subject.

    Returns:
        Optional[int]: Identifier of the created subject in the database, or None if an error occurred.
    """
    try:
        subject = Subject(name=name, teacher_id=teacher_id)
        session.add(subject)
        session.flush()
        subject_id = subject.id
        session.commit()
        logger.log(f"New subject {name} added successfully!")
        return subject_id
    except Exception as e:
        session.rollback()
        logger.log(f"An error occurred while creating subject: {e}", level=40)
        return None
    finally:
        session.close()


def create_grade(
    grade_value: int, student_id: int, subject_id: int, grade_date: str
) -> None:
    """
    Creates a new grade in the database.

    Args:
        grade_value (int): Grade value.
        student_id (int): Identifier of the student who receives the grade.
        subject_id (int): Identifier of the subject for which the grade is given.
        grade_date (str): Grade date in 'YYYY-MM-DD' format.
    """
    try:
        date = datetime.strptime(grade_date, "%Y-%m-%d")
        grade = Grade(
            grade=grade_value,
            student_id=student_id,
            subject_id=subject_id,
            grade_date=date,
        )
        session.add(grade)
        session.commit()
        logger.log(f"New grade {grade_value} added successfully!")
    except Exception as e:
        session.rollback()
        logger.log(f"An error occurred while creating grade: {e}", level=40)
    finally:
        session.close()


def list_records(model: Union[Teacher, Group, Student, Subject, Grade]) -> None:
    """
    Prints all records of the specified model from the database.

    Args:
        model (Union[Teacher, Group, Student, Subject, Grade]): SQLAlchemy model.
    """
    try:
        records = session.query(model).all()
        logger.log(f"All records {model.__tablename__}")
        for record in records:
            record_dict = record.__dict__
            record_values = ", ".join(
                [
                    f"{key}={value}"
                    for key, value in record_dict.items()
                    if not key.startswith("_") and not callable(value)
                ]
            )
            logger.log(f"{model.__name__}: {record_values}")
    except Exception as e:
        logger.log(f"An error occurred while listing records: {e}", level=40)
    finally:
        session.close()


def update_record(
    model: Union[Teacher, Group, Student, Subject, Grade],
    record_id: int,
    field_name: str,
    name: str,
) -> None:
    """
    Updates a record in the database.

    Args:
        model (Union[Teacher, Group, Student, Subject, Grade]): SQLAlchemy model for updating the record.
        record_id (int): Identifier of the record to update.
        field_name (str): Name of the field to update.
        name (str): New value for the field.
    """
    try:
        record = session.query(model).filter_by(id=record_id).first()
        if record:
            setattr(record, field_name, name)
            session.commit()
            logger.log(f"Record in {model} with id {record_id} updated successfully!")
        else:
            logger.log(f"No {model.__name__} found with id {record_id}")
    except Exception as e:
        session.rollback()
        logger.log(
            f"An error occurred while updating record with id {record_id}: {e}",
            level=40,
        )
    finally:
        session.close()


def remove_record(
    model: Union[Teacher, Group, Student, Subject, Grade], record_id: int
) -> None:
    """
    Removes a record from the database.

    Args:
        model (Union[Teacher, Group, Student, Subject, Grade]): SQLAlchemy model for removing the record.
        record_id (int): Identifier of the record to remove.
    """
    try:
        record = session.query(model).filter_by(id=record_id).first()
        if record:
            session.delete(record)
            session.commit()
            logger.log(f"Record in {model} with id {record_id} removed successfully!")
        else:
            logger.log(f"No {model.__name__} found with id {record_id}")
    except Exception as e:
        session.rollback()
        logger.log(
            f"An error occurred while removing record with id {record_id}: {e}",
            level=40,
        )
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="CLI program for interacting with the database"
    )
    parser.add_argument(
        "--action",
        "-a",
        choices=["create", "list", "update", "remove"],
        help="Action to perform",
    )
    parser.add_argument(
        "--model",
        "-m",
        choices=["Teacher", "Group", "Student", "Subject", "Grade"],
        help="Model to interact with",
    )
    parser.add_argument("--id", type=int, help="Record identifier")
    parser.add_argument("--name", "-n", help="Name for creation or update")
    parser.add_argument("--group_id", type=int, help="Group identifier (for student)")
    parser.add_argument(
        "--teacher_id", type=int, help="Teacher identifier (for subject)"
    )
    parser.add_argument(
        "--field_name", type=str, help="Field identifier (for updating field)"
    )
    parser.add_argument(
        "--grade_value", type=int, help="Grade value (for creating grade)"
    )
    parser.add_argument(
        "--grade_date", type=str, help="Grade date value (for creating grade)"
    )
    parser.add_argument(
        "--student_id", type=int, help="Student identifier (for creating grade)"
    )
    parser.add_argument(
        "--subject_id", type=int, help="Subject identifier (for creating grade)"
    )

    args = parser.parse_args()

    if args.action == "create":
        if args.model == "Teacher":
            create_teacher(args.name)
        elif args.model == "Group":
            create_group(args.name)
        elif args.model == "Student":
            create_student(args.name, args.group_id)
        elif args.model == "Subject":
            create_subject(args.name, args.teacher_id)
        elif args.model == "Grade":
            create_grade(
                args.grade_value, args.student_id, args.subject_id, args.grade_date
            )
    elif args.action == "list":
        if args.model:
            model = eval(args.model)
            list_records(model)
    elif args.action == "update":
        if args.model and args.id and args.field_name and args.name:
            model = eval(args.model)
            update_record(model, args.id, args.field_name, args.name)
    elif args.action == "remove":
        if args.model and args.id:
            model = eval(args.model)
            remove_record(model, args.id)
