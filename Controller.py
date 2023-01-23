import View
import Model
import time
import os

while 1:
    View.menu()
    choice = input("Оберіть варіант: ")
    model = Model.db_model("lab", "postgres", "111", "")

    match choice:
        case "1":
            mas = model.get_table_names()
            View.show(mas)
            time.sleep(4)
        case "2":
            table = input("Введіть назву таблиці: ")
            mas = model.get_column_types(table)
            View.show(mas)
            time.sleep(4)
        case "3":
            table = input("Введіть назву таблиці: ")
            mas = model.get_column_names(table)
            View.show(mas)
            time.sleep(4)
        case "4":
            table = input("Введіть назву таблиці: ")
            mas = model.get_foreign_key_info(table)
            View.show(mas)
            time.sleep(4)
        case "5":
            table = input("Введіть назву таблиці: ")
            count = input("Введіть count: ")
            model.generate_data(table, count)
            mas = model.get_table_data(table)
            View.show(mas)
            time.sleep(4)
        case "6":
            table = input("Введіть назву таблиці: ")
            columns = input("Введіть назви колонок: ").split(' ')
            val = input("Введіть відповідні значення: ").split(' ')
            values = {key:value for (key,value) in zip(columns,val)}
            model.insert_data(table, values)
            print("result:\n")
            mas = model.get_table_data(table)
            View.show(mas)
            time.sleep(4)
        case "7":
            table = input("Введіть назву таблиці: ")
            columns = input("Введіть назви колонок: ").split(' ')
            val = input("Введіть відповідні значення: ").split(' ')
            values = {key:value for (key,value) in zip(columns,val)}
            model.change_data(table, values)
            mas = model.get_table_data(table)
            View.show(mas)
            time.sleep(4)
        case "8":
            table = input("Введіть назву таблиці: ")
            column = input("Назва колонки: ")
            atribut = input("Параметр: ")
            model.delete_data(table, column, atribut)
            mas = model.get_table_data(table)
            View.show(mas)
            time.sleep(4)
        case "9":
            table = input("Введіть назву таблиці: ")
            mas = model.get_table_data(table)
            View.show(mas)
            time.sleep(4)
        case "10":
            table = input("Введіть назву таблиці: ")
            mas = model.get_table_data(table)
            View.show(mas)
            time.sleep(4)
        case "11":
            os.system('cls||clear')
            break

        case _:
            print("Choose correct values in 1-11")
            time.sleep(4)




