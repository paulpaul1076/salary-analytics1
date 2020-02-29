**Аналитика зарплат.**

**Чеклист задания:**
1) Написано на spark + scala
2) Json файлы читаются в параллель
3) Задание записывает в csv список всех работников c полями : name, salary, department
4) Задание выводит на экран работника с самой большой зарплатой
5) Задание выводит на экран сумму зарплат всех работников
6) Json парсится с помощью библиотеки json4s
7) spark.read.json/text для чтения файлов не используется

Так же все покрыто unit/integration тестами.

**Как собирать:**
Командой "sbt assembly" (я собирал с версией sbt 0.13.8).

**Пример команды для запуска:**
./spark-submit --driver-class-path SalaryAnalytics.jar --master <ВАШ МАСТЕР УРЛ> --class com.company.Solution SalaryAnalytics.jar --paths /emp1/developers/111.json,/emp1/hr/555.json --csv /path/to/csv

