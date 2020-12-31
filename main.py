import queue ,random
import psycopg2
import threading

q = queue.Queue()

class DBManager:


    def __init__(self):
        try:
            self.conn = psycopg2.connect("dbname=test user=damian")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def __del__(self):
        self.conn.close()

    def internet_database(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS mobile")
            create_table_query = '''CREATE TABLE mobile
                      (ID INT PRIMARY KEY     NOT NULL,
                      MODEL           TEXT    NOT NULL,
                      PRICE         REAL); '''
            # Execute a command: this creates a new table
            cursor.execute(create_table_query)
            self.conn.commit()
            print("Table created successfully in PostgreSQL ")
            insert_query = """ INSERT INTO mobile (ID, MODEL, PRICE) VALUES (1, 'Iphone12', 1100)"""
            cursor.execute(insert_query)
            postgreSQL_select_Query = "select * from mobile where id = %s"
            mobileID = 1
            cursor.execute(postgreSQL_select_Query, (mobileID,))
            mobile_records = cursor.fetchall()
            # print(mobile_records)

            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while connecting to PostgreSQL", error)

    def delete_db(self):
        try:
            cur = self.conn.cursor()
            cur.execute("DROP TABLE IF EXISTS sweets CASCADE")
            print(cur.rowcount)
            cur.execute("DROP TABLE IF EXISTS packs CASCADE")
            print(cur.rowcount)
            cur.execute("DROP TABLE IF EXISTS pack_sweet CASCADE")
            print(cur.rowcount)
            cur.execute("DROP TABLE IF EXISTS sweet_resemblance CASCADE")
            print(cur.rowcount)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


    def create_db(self):
        commands = ("""CREATE TABLE IF NOT EXISTS sweets (
                        my_name text ,
                        number_left int,
                        more_than_0 int check (number_left > 0),
                        PRIMARY KEY(my_name)
                    );""",
                    """CREATE TABLE IF NOT EXISTS packs (
                        id int,
                        country text,
                        recipent text,
                        PRIMARY KEY(id)
                    );""",
                    """CREATE TABLE IF NOT EXISTS pack_sweet (
                        pack_id int,
                        sweet text,
                        number int,
                        CONSTRAINT fk_pack_id
                            FOREIGN KEY(pack_id)
                                REFERENCES packs(id)
                    );""",
                    """CREATE TABLE IF NOT EXISTS sweet_resemblance (
                        sweet_1 text,
                        sweet_2 text,
                        resemblance decimal,
                        CONSTRAINT fk_sweet_1
                        FOREIGN KEY(sweet_1)
                        REFERENCES sweets(my_name),
                        CONSTRAINT fk_sweet_2
                        FOREIGN KEY(sweet_2)
                        REFERENCES sweets(my_name)
                    );""")

        try:
            cur = self.conn.cursor()
            for command in commands:
                cur.execute(command)
            cur.close()
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


    def fill_db(self, sweets_list, resemblance_list):
        sweets_insert_command = ("INSERT INTO sweets(my_name, number_left, more_than_0) VALUES(%s, %s, %s);")
        resemblance_insert_command = ("""INSERT INTO sweet_resemblance(sweet1, sweet2, resemblance) VALUES(%s, %s, %s);""")

        print(sweets_list)
        try:
            cur = self.conn.cursor()

            cur.executemany(sweets_insert_command, sweets_list)
            # cur.executemany(resemblance_insert_command, resemblance_list)
            cur.close()
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def set_isolation_level(self, level):
        isolation_levels = {
            "SERIALIZABLE": psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE,
            "REPEATABLE_READ": psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            "READ_COMMITTED": psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
            "READ_UNCOMMITTED": psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED
        }

        self.conn.set_isolation_level(isolation_levels[level])


    def do_transaction(self, id, country, recipent, sweets_list):
        try:
            # todo change me
            self.set_isolation_level("SERIALIZABLE")

            cur = self.conn.cursor()

            print("HELLO")
            postgreSQL_select_Query = "select * from mobile where id = %s"
            mobileID = 1
            cur.execute(postgreSQL_select_Query, (mobileID,))
            mobile_records = cur.fetchall()
            print(mobile_records)


            cur.execute("INSERT INTO packs(id, country, recipent) VALUES(%s, %s, %s)", (id, country, recipent));
            assert (cur.rowcount == 1)

            # przyjmujemy zalozenie, ze wysylamy paczki jednego typu
            for sweet in sweets_list:
                found_sweet = False

                has_left = 100
                cur.execute("SELECT * FROM sweets WHERE number_left = %s;", (has_left,))
                sweets_in_magazine = cur.fetchone()

                # Mamy wlasciwy slodycz
                if sweets_in_magazine >= sweet["number"]:
                    cur.execute("INSERT INTO pack_sweet (%s, %s, %s)", id, similar_sweet, sweet["number"])
                    cur.execute("UPDATE sweets SET number_left = number_left - %s WHERE my_name = %s", sweet["number"], sweet["name"])
                    continue

                # Szukamy podobnego slodycza
                cur.execute("SELECT sweet_2 FROM sweet_resemblance WHERE sweet_1 == %s ORDER BY resemblance DESC", sweet["name"])
                similar_sweets = cur.fetchall()

                for similar_sweet in similar_sweets:
                    cur.execute("SELECT number_left FROM sweets WHERE my_name = %s", similar_sweet)
                    similar_sweet_in_magazine = cur.fetchone()
                    if similar_sweet_in_magazine > sweet["number"]:
                        found_sweet = True
                        cur.execute("INSERT INTO pack_sweet (%s, %s, %s)", id, similar_sweet, sweet["number"])
                        cur.execute("UPDATE sweets SET number_left = number_left - %s WHERE my_name = %s", sweet["number"], similar_sweet)
                        break

                if not found_sweet:
                    self.conn.rollback()
                    return False

            self.conn.commit()
            return True

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


class DataGenerator:
    def __init__(self):
        self.sweets = ["chocolate", "lollypop", "biscuit", "cake"]
        self.countries = ["Poland, Germany, France, UK, USA, Norway, Sweden"]
        self.names = ["Damian", "John", "Steve", "Mary", "Anne", "Jane"]

    def get_sweets(self):
        sweets_list = []
        sweets_list.append(('chocolate', 100, 1,))
        sweets_list.append(('lollypop', 100, 1,))
        sweets_list.append(('biscuit', 100, 1,))
        sweets_list.append(('cake', 100, 1,))
        return sweets_list

    def get_resemblance(self):
        resemblane_list = []
        resemblane_list.append(('chocolate', 'lollypop', 0.5))
        resemblane_list.append(('lollypop', 'chocolate', 0.5))
        resemblane_list.append(('biscuit', 'cake', 0.5))
        resemblane_list.append(('cake', 'biscuit', 0.5))
        return resemblane_list

    def fill_sweets_queue(self):
        for i in range (1, 100):
            q.put({"id": random.randrange(10), "number": random.randrange(50), "sweet": random.choice(self.sweets)})

    def fill_sweets_queue_2(self):
        for i in range (1, 1000):
            l = []
            items_on_list = random.randrange(4)
            for _ in range (1, 3):
                l.append({"number": random.randrange(20), "name": random.choice(self.sweets)})
            q.put({"id": i, "country": random.choice(self.countries), "recipient": random.choice(self.names), "list": l})

def worker():
    letter = q.get()
    manager = DBManager()
    manager.do_transaction(letter["id"], letter["country"], letter["recipient"], letter["list"])


if __name__ == '__main__':
    print(psycopg2.__version__)

    manager = DBManager()
    data_generator = DataGenerator()

    manager.internet_database()
    manager.delete_db()
    manager.create_db()
    manager.fill_db(data_generator.get_sweets(), data_generator.get_resemblance())

    data_generator.fill_sweets_queue_2()

    worker()
    # threads = []
    # for _ in range(1, 2):
    #     t = threading.Thread(target=worker)
    #     threads.append(t)
    #     t.start()
