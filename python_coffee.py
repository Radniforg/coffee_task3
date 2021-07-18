import psycopg2
import conditions


def table_clean(dbname=conditions.database, user=conditions.user, password=conditions.password, host=conditions.host):
    conn = psycopg2.connect(dbname=dbname, user=user,
                            password=password, host=host)
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS outlets_clean;")
        cur.execute("CREATE TABLE outlets_clean (id int NOT NULL, "
                    "Торг_точка_чистый_адрес varchar(256) DEFAULT NULL);")
        cur.close()


def table_select(dbname=conditions.database, user=conditions.user, password=conditions.password, host=conditions.host):
    conn = psycopg2.connect(dbname=dbname, user=user,
                            password=password, host=host)
    with conn:
        cur = conn.cursor()
        cur.execute("SELECT * "
                    "FROM outlets "
                    "ORDER BY id")
        dirty_data = cur.fetchall()
        cur.close()

    return dirty_data


def data_sorting(data):
    for_old_table = {}
    clean = {}
    new_id = 1
    void_address = ['-', 'БА', 'Б/А', r'б\\а', 'б/адреса', 'он же', 'б/а', '1', '0', 'Ж']
    for tup in data:
        for_old_table[tup[0]] = {}
        for_old_table[tup[0]]['city'] = tup[1]
        # формально нас не интересует в рамках задачи, всё Ростов, проверять не будем.
        for_old_table[tup[0]]['name'] = tup[2]
        if tup[3] in void_address:
            for_old_table[tup[0]]['address'] = 'void'
        else:
            for_old_table[tup[0]]['address'] = tup[3]
        if tup[2] in clean.keys():
            # если адрес новой строки совпадает с адресом в словаре, то это дубль,
            # просто приписываем дублю тот же id
            if clean[tup[2]]['address'] == for_old_table[tup[0]]['address']:
                for_old_table[tup[0]]['new_id'] = clean[tup[2]]['new_id']
                clean[tup[2]]['list'].append(tup[0])
            else:
                # если адрес относится к классу пустых, то тоже тупо пишем дублю id
                if for_old_table[tup[0]]['address'] == 'void':
                    for_old_table[tup[0]]['new_id'] = clean[tup[2]]['new_id']
                    clean[tup[2]]['list'].append(tup[0])
                # если старый адрес относился к пустым, то объявляем новый основным, но пишем тот же дублевый id
                elif clean[tup[2]]['address'] == 'void':
                    clean[tup[2]]['address'] = for_old_table[tup[0]]['address']
                    for_old_table[tup[0]]['new_id'] = clean[tup[2]]['new_id']
                    clean[tup[2]]['list'].append(tup[0])
                else:
                    # если оба адреса непустые,
                    # то это конфликт адресов. Объявляем невозможность дедупликации, id становится None.
                    # Поскольку это относится ко всем точкам с этим адресом, то все предыдущие точки (через list)
                    # тоже получают id None.
                    # В результате такого мы можем получить перескок через id, но это уже вопрос последующей чистки
                    for sales in clean[tup[2]]['list']:
                        for_old_table[sales]['new_id'] = None
                    clean[tup[2]]['list'].append(tup[0])
                    clean[tup[2]]['new_id'] = None
                    for_old_table[tup[0]]['new_id'] = clean[tup[2]]['new_id']
        else:
            clean[tup[2]] = {}
            # новая точка задает всему потоку новый адрес, начинает список точек группы,
            # потоку выдается общий новый id, id листается дальше.
            clean[tup[2]]['address'] = for_old_table[tup[0]]['address']
            clean[tup[2]]['list'] = [tup[0]]
            clean[tup[2]]['new_id'] = new_id
            for_old_table[tup[0]]['new_id'] = new_id
            new_id += 1
    return for_old_table, clean


def table_clean_insert(values, dbname=conditions.database, user=conditions.user,
                       password=conditions.password, host=conditions.host):
    conn = psycopg2.connect(dbname=dbname, user=user,
                            password=password, host=host)
    with conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO outlets_clean (id, "
                    "Торг_точка_чистый_адрес) VALUES (%s, %s)", values)


def table_old_insert(values, dbname=conditions.database, user=conditions.user,
                     password=conditions.password, host=conditions.host):
    conn = psycopg2.connect(dbname=dbname, user=user,
                            password=password, host=host)
    with conn:
        cur = conn.cursor()
        cur.execute("UPDATE outlets SET outlet_clean_id = %s "
                    "WHERE id = %s", values)


table_clean()
total_clean = data_sorting(table_select())
old_table = total_clean[0]
clean_base = total_clean[1]
true_new_id = 1
timer = 0
for keys in clean_base:
    timer += 1
    if clean_base[keys]["new_id"]:
        if clean_base[keys]["address"] != 'void':
            table_clean_insert((true_new_id, clean_base[keys]["address"]))
            for sales in clean_base[keys]['list']:
                old_table[sales]['new_id'] = true_new_id
            print(f'outlets_clean filling in process: {timer/len(clean_base) * 100:0.2f} %')
            true_new_id += 1
for key in old_table:
    table_old_insert((old_table[key]["new_id"], key))
    print(f'outlets filling in process: {key/len(old_table) * 100:0.2f} %')



