from datetime import datetime
import sys
import os


from sqlalchemy.exc import SQLAlchemyError


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from conf.models import Student, Group, Teacher, Subject, Grade, Base
from customlogger import logger
from conf.db import session
from faker import Faker
import random


fake = Faker()


def seed_data(session) -> None:
    """
    Fills the database with random data.

    Args:
        session: Database session SQLAlchemy.

    Returns:
        None
    """
    groups = ["Group A", "Group B", "Group C"]
    for group_name in groups:
        group = Group(name=group_name)
        session.add(group)

    teachers = [fake.name() for _ in range(8)]
    for teacher_name in teachers:
        teacher = Teacher(fullname=teacher_name)
        session.add(teacher)

    subjects = [
        "Mathematics",
        "Physics",
        "Chemistry",
        "Biology",
        "History",
        "Geograph",
        "Literature",
        "Computer Science",
    ]
    for teacher_id, subject_name in enumerate(subjects, start=1):
        subject = Subject(name=subject_name, teacher_id=teacher_id)
        session.add(subject)

    for _ in range(50):
        student_name = fake.name()
        group_id = random.randint(1, len(groups))
        student = Student(fullname=student_name, group_id=group_id)
        session.add(student)

    for student_id in range(1, 51):
        for subject_id in range(1, 9):
            for _ in range(3):
                grade = random.randint(60, 100)
                date = datetime(
                    2023, random.randint(1, 12), random.randint(1, 27)
                ).date()
                grade_entry = Grade(
                    student_id=student_id,
                    subject_id=subject_id,
                    grade=grade,
                    grade_date=date,
                )
                session.add(grade_entry)


if __name__ == "__main__":
    try:
        seed_data(session)
        session.commit()
        logger.log("Data seeded successfully!")
    except SQLAlchemyError as e:
        logger.log(f"SQLAlchemyError occurred while seeding data: {e}", level=40)
        session.rollback()
    except Exception as e:
        logger.log(f"Error occurred while seeding data: {e}", level=40)
        session.rollback()
    finally:
        session.close()
