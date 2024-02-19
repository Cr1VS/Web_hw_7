from sqlalchemy import Integer, String, ForeignKey, Date
from sqlalchemy.orm import Mapped, relationship, declarative_base, mapped_column


Base = declarative_base()


class Teacher(Base):
    __tablename__ = "teachers"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fullname: Mapped[str] = mapped_column(String(70), nullable=False)


class Group(Base):
    __tablename__ = "groups"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(70), nullable=False)


class Student(Base):
    __tablename__ = "students"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fullname: Mapped[str] = mapped_column(String(70), nullable=False)
    group_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("groups.id", ondelete="CASCADE")
    )
    group = relationship("Group", backref="students")


class Subject(Base):
    __tablename__ = "subjects"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    teacher_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("teachers.id", ondelete="CASCADE")
    )
    teacher = relationship("Teacher", backref="disciplines")


class Grade(Base):
    __tablename__ = "grades"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    grade: Mapped[int] = mapped_column(Integer, nullable=False)
    grade_date: Mapped[Date] = mapped_column(Date, nullable=True)
    student_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("students.id", ondelete="CASCADE")
    )
    subject_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subjects.id", ondelete="CASCADE")
    )
    student = relationship("Student", backref="grades")
    subject = relationship("Subject", backref="grades")
