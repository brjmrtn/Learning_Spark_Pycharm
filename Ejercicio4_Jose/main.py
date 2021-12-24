from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

passKey = '**********'

# Cargar con Spark los datos de employees y departamento
spark = SparkSession.builder.appName('Capitulo05').getOrCreate()

employees_df = (spark.read.format('jdbc')
                .option('url', 'jdbc:mysql://localhost:3306/employees')
                .option('driver', 'com.mysql.cj.jdbc.Driver')
                .option('dbtable', 'employees')
                .option('user', 'root')
                .option('password', passKey)
                .load())

departments_df = (spark.read.format('jdbc')
                  .option('url', 'jdbc:mysql://localhost:3306/employees')
                  .option('driver', 'com.mysql.cj.jdbc.Driver')
                  .option('dbtable', 'departments')
                  .option('user', 'root')
                  .option('password', passKey)
                  .load())

# Mediante Joins mostrar toda la información de los empleados además de su título y salario
salaries_df = (spark.read.format('jdbc')
               .option('url', 'jdbc:mysql://localhost:3306/employees')
               .option('driver', 'com.mysql.cj.jdbc.Driver')
               .option('dbtable', 'salaries')
               .option('user', 'root')
               .option('password', passKey)
               .load())

title_df = (spark.read.format('jdbc')
            .option('url', 'jdbc:mysql://localhost:3306/employees')
            .option('driver', 'com.mysql.cj.jdbc.Driver')
            .option('dbtable', 'titles')
            .option('user', 'root')
            .option('password', passKey)
            .load())
deptemp_df = (spark.read.format('jdbc')
              .option('url', 'jdbc:mysql://localhost:3306/employees')
              .option('driver', 'com.mysql.cj.jdbc.Driver')
              .option('dbtable', 'dept_emp')
              .option('user', 'root')
              .option('password', passKey)
              .load())

# JOINS
fulldf = (employees_df
          .join(salaries_df, salaries_df.emp_no == employees_df.emp_no)
          .join(title_df, title_df.emp_no == employees_df.emp_no)
          .join(deptemp_df, deptemp_df.emp_no == employees_df.emp_no)
          .select(employees_df.first_name, employees_df.last_name, salaries_df.salary,
                  title_df.title, deptemp_df.dept_no, deptemp_df.from_date))

fulldf.show(truncate=False)

# Diferencias entre Rank vs dense_rank
"""Se utilizan para ordenar valores y asignar números según el orden en el que se desee.
La diferencia está en como manejan los valores idénticos:
Los dos asignan a valores idénticos el mismo rango, pero RANK omitirá el siguiente valor disponible
y DENSE_RANK seguiria el orden cronologico con el siguiente valor"""

# Utlizando operaciones ventana obtener salario, cargo, y departamento actual.

employee_dept_df = (fulldf
                    .join(departments_df, departments_df.dept_no == fulldf.dept_no)
                    .select(fulldf.emp_no, fulldf.first_name, fulldf.last_name,
                            fulldf.title, fulldf.salary, departments_df.dept_name,
                            fulldf.from_date,
                            ))

w = Window.partitionBy('emp_no', 'first_name', 'last_name').orderBy(desc('from_date'))

window = (employee_dept_df.select(col('emp_no'), col('first_name'),
                                  col('last_name'), col('title'), col('salary'),
                                  col('dept_name'), col('from_date'))
          .withColumn('rn', row_number().over(w))
          .where(col('rn') == lit(1)).drop(col('rn'))
          .orderBy(col('emp_no')))

window.show()
