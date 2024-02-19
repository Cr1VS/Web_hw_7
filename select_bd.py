from sqlalchemy import func, desc, select, and_, cast, String, Integer


from collections import defaultdict
from tabulate import tabulate
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from conf.models import Grade, Teacher, Student, Group, Subject
from customlogger import logger
from conf.db import session


def select_01() -> str:
    """
    Selects the top 5 students with the highest average grade.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        result = (
            session.query(
                Student.id,
                Student.fullname,
                func.round(func.avg(Grade.grade)).label("average_grade"),
            )
            .select_from(Student)
            .join(Grade)
            .group_by(Student.id)
            .order_by(desc("average_grade"))
            .limit(5)
            .all()
        )
        if result is not None:
            table_data = [[i[1], i[2]] for i in result]

            headers = ["Student Name", "Average Grade"]

            table = tabulate(table_data, headers=headers, tablefmt="grid")
            logger.log(
                f"Selects the top 5 students with the highest average grade successfully!"
            )
            return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_02() -> str:
    """
    Selects the top student with the highest average grade in mathematics.

    Returns:
        str: A formatted table containing the result.
    """
    try:
        result = (
            session.query(
                Student.id,
                Student.fullname,
                func.round(func.avg(Grade.grade)).label("average_grade"),
            )
            .join(Grade)
            .filter(Grade.subject_id == 1)
            .group_by(Student.id)
            .order_by(desc("average_grade"))
            .first()
        )
        if result is not None:
            student_id, student_name, average_grade = result
            table_data = [[student_name, "Mathematics", average_grade]]
        else:
            table_data = []

        headers = ["Student Name", "Subject Name", "Average Grade"]
        logger.log(
            f"Selects the top student with the highest average grade in mathematics successfully!"
        )
        table = tabulate(table_data, headers=headers, tablefmt="grid")
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_03() -> str:
    """
    Selects the average grades for each group in mathematics.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        result = (
            session.query(
                Group.name, func.round(func.avg(Grade.grade)).label("average_grade")
            )
            .select_from(Group)
            .join(Student)
            .join(Grade)
            .filter(Grade.subject_id == 1)
            .group_by(Group.name)
            .order_by(desc("average_grade"))
            .all()
        )
        table_data = [[i[0], "Mathematics", i[1]] for i in result]

        headers = ["Group Name", "Subject Name", "Average Grade"]

        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(
            f"Selects the average grades for each group in mathematics successfully!"
        )
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_04() -> str:
    """
    Selects the average grade of all students.

    Returns:
        str: A formatted table containing the result.
    """
    try:
        result = session.query(
            func.round(func.avg(Grade.grade)).label("average_grade")
        ).scalar()
        if result is not None:
            average_grade = result
            table_data = [[average_grade]]
        else:
            table_data = []

        headers = ["Average Grade"]

        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(f"Selects the average grade of all students successfully!")

        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_05() -> str:
    """
    Selects the subjects taught by a specific teacher.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        result = (
            session.query(Teacher.fullname, Subject.name)
            .join(Teacher)
            .filter(Teacher.id == 1)
            .all()
        )
        table_data = [[i[0], i[1]] for i in result]

        headers = ["Teacher Name", "Subject Name"]

        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(f"Selects the subjects taught by a specific teacher successfully!")
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_06() -> str:
    """
    Selects the students belonging to a specific group.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        result = (
            session.query(Group.name, Student.fullname)
            .join(Group)
            .filter(Group.name == "Group A")
            .all()
        )
        table_data = [[i[0], i[1]] for i in result]

        headers = ["Group Name", "Student Name"]

        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(f"Selects the students belonging to a specific group successfully!")
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_07() -> str:
    """
    Selects the students in Group A with their grades in mathematics concatenated.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        result = (
            session.query(
                Student.fullname,
                Subject.name,
                func.string_agg(cast(Grade.grade, String), ", ").label("grades_concat"),
            )
            .select_from(Student)
            .join(Grade)
            .join(Group)
            .join(Subject)
            .filter(Group.name == "Group A")
            .filter(Subject.name == "Mathematics")
            .group_by(Student.fullname, Subject.name)
            .order_by(desc(func.sum(func.cast(Grade.grade, Integer))))
            .all()
        )

        table_data = [[i[0], i[1], i[2]] for i in result]

        headers = ["Student Name", "Subject Name", "Grade"]

        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(
            f"Selects the students in Group A with their grades in mathematics concatenated successfully!"
        )
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_08() -> str:
    """
    Selects the average grade for each subject taught by a specific teacher.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        result = (
            session.query(
                Teacher.fullname, Subject.name, func.round(func.avg(Grade.grade))
            )
            .select_from(Teacher)
            .join(Subject)
            .join(Grade)
            .filter(Subject.teacher_id == 2)
            .group_by(Teacher.fullname, Subject.name)
            .all()
        )

        table_data = [[i[0], i[1], i[2]] for i in result]

        headers = ["Teacher Name", "Subject Name", "Average Grade"]

        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(
            f"Selects the average grade for each subject taught by a specific teacher successfully!"
        )
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_09() -> str:
    """
    Selects the subjects taken by a specific student.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        result = (
            session.query(
                Student.fullname, Group.name, Subject.name.label("subject_name")
            )
            .join(Group, Student.group_id == Group.id)
            .join(Grade, Student.id == Grade.student_id)
            .join(Subject, Grade.subject_id == Subject.id)
            .filter(Student.id == 3)
            .distinct()
            .all()
        )

        student_subjects = defaultdict(list)
        for i in result:
            student_name = i[0]
            subject_name = i[2]
            student_subjects[student_name].append(subject_name)

        table_data = []
        for student_name, subjects in student_subjects.items():
            group_name = result[0][1]
            subjects_str = ", ".join(subjects)
            table_data.append([group_name, student_name, subjects_str])

        headers = ["Group Name", "Student Name", "Subject Names"]
        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(f"Selects the subjects taken by a specific student successfully!")
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_10() -> str:
    """
    Selects the subjects and teacher of a specific student.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        result = (
            session.query(Subject.name, Student.fullname, Teacher.fullname)
            .join(Grade, Subject.id == Grade.subject_id)
            .join(Student, Grade.student_id == Student.id)
            .join(Teacher, Subject.teacher_id == Teacher.id)
            .filter(Student.id == 3)
            .filter(Teacher.id == 1)
            .distinct()
            .all()
        )
        table_data = [[i[1], i[0], i[2]] for i in result]

        headers = ["Student Name", "Subject Name", "Teacher Name"]

        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(
            f"Selects the subjects and teacher of a specific student successfully!"
        )
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_11() -> str:
    """
    Selects the average grade for each student in a specific subject taught by a specific teacher.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        result = (
            session.query(
                Student.fullname, Teacher.fullname, Subject.name, func.avg(Grade.grade)
            )
            .join(Grade, Student.id == Grade.student_id)
            .join(Subject, Grade.subject_id == Subject.id)
            .join(Teacher, Subject.teacher_id == Teacher.id)
            .filter(Teacher.id == 1)
            .filter(Student.id == 5)
            .group_by(Student.fullname, Teacher.fullname, Subject.name)
            .all()
        )
        table_data = result

        headers = ["Student Name", "Teacher Name", "Subject Name", "Average Grade"]

        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(
            f"Selects the average grade for each student in a specific subject taught by a specific teacher successfully!"
        )
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


def select_12() -> str:
    """
    Selects the latest grade for each student in a specific subject within a specific group.

    Returns:
        str: A formatted table containing the results.
    """
    try:
        subquery = (
            select(func.max(Grade.grade_date))
            .join(Student)
            .filter(and_(Grade.subject_id == 2, Student.group_id == 3))
            .scalar_subquery()
        )

        result = (
            session.query(
                Student.id,
                Group.name,
                Student.fullname,
                Subject.name,
                Grade.grade,
                Grade.grade_date,
            )
            .select_from(Grade)
            .join(Student)
            .join(Group)
            .filter(
                and_(Grade.subject_id == 2, Group.id == 3, Grade.grade_date == subquery)
            )
            .order_by(Grade.grade.desc())
            .all()
        )
        table_data = [[i[1], i[2], i[3], i[4], i[5]] for i in result]

        headers = ["Group Name", "Student Name", "Subject Name", "Grade", "Grade date"]

        table = tabulate(table_data, headers=headers, tablefmt="grid")
        logger.log(
            f"Selects the latest grade for each student in a specific subject within a specific group successfully!"
        )
        return table
    except Exception as e:
        logger.log(f"An error occurred: {e}", level=40)
        return ""


if __name__ == "__main__":
    logger.log(select_01())
    logger.log(select_02())
    logger.log(select_03())
    logger.log(select_04())
    logger.log(select_05())
    logger.log(select_06())
    logger.log(select_07())
    logger.log(select_08())
    logger.log(select_09())
    logger.log(select_10())
    logger.log(select_11())
    logger.log(select_12())
