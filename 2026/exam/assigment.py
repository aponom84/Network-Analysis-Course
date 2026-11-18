import pandas as pd
import random

# Чтение списка студентов
students_df = pd.read_csv('students.csv')
students = students_df['Имя студента'].tolist()

# Чтение вопросов из markdown файла
with open('Exam-Questions.md', 'r', encoding='utf-8') as f:
    content = f.read()

# Парсинг вопросов
questions = []
lines = content.split('\n')
for line in lines:
    line = line.strip()
    if line and line[0].isdigit() and '. ' in line:
        # Извлекаем номер и текст вопроса
        parts = line.split('. ', 1)
        if len(parts) == 2:
            question_num = parts[0]
            question_text = parts[1]
            questions.append(f"{question_num}. {question_text}")

print(f"Найдено {len(questions)} вопросов")
print(f"Найдено {len(students)} студентов")

# Проверка, что вопросов достаточно
if len(questions) < 2:
    print("Ошибка: недостаточно вопросов для назначения")
    exit()

# Случайное назначение 2 уникальных вопросов каждому студенту
results = []

for student in students:
    # Выбираем 2 случайных уникальных вопроса
    selected_questions = random.sample(questions, 2)
    
    results.append({
        'Студент': student,
        'Тема 1': selected_questions[0],
        'Тема 2': selected_questions[1]
    })

# Создаем DataFrame и сохраняем в CSV
result_df = pd.DataFrame(results)
result_df.to_csv('exam_assignment.csv', index=False, encoding='utf-8-sig')

print("Назначение вопросов завершено!")
print(f"Результат сохранен в файл: exam_assignment.csv")
print("\nПример первых 5 назначений:")
print(result_df.head().to_string(index=False))