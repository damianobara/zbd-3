import queue ,random
import psycopg2
import threading
import string
import time

q = queue.Queue()
lock = threading.Lock()


class DBManager:


    def __init__(self):
        try:
            self.conn = psycopg2.connect("dbname=test user=damian")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def __del__(self):
        self.conn.close()


    def delete_db(self):
        try:
            cur = self.conn.cursor()
            cur.execute("DROP TABLE IF EXISTS sweets CASCADE")
            cur.execute("DROP TABLE IF EXISTS packs CASCADE")
            cur.execute("DROP TABLE IF EXISTS pack_sweet CASCADE")
            cur.execute("DROP TABLE IF EXISTS sweet_resemblance CASCADE")
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


    def create_db(self):
        commands = ("""CREATE TABLE IF NOT EXISTS sweets (
                        name text ,
                        has_left int,
                        check (has_left >= 0),
                        PRIMARY KEY(name)
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
                        REFERENCES sweets(name),
                        CONSTRAINT fk_sweet_2
                        FOREIGN KEY(sweet_2)
                        REFERENCES sweets(name)
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
        sweets_insert_command = ("INSERT INTO sweets(name, has_left) VALUES(%s, %s);")
        resemblance_insert_command = ("""INSERT INTO sweet_resemblance(sweet_1, sweet_2, resemblance) VALUES(%s, %s, %s);""")

        try:
            cur = self.conn.cursor()

            cur.executemany(sweets_insert_command, sweets_list)
            cur.executemany(resemblance_insert_command, resemblance_list)
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


    def do_transaction_no_retry(self, id, country, recipent, sweets_list):
        try:
            # todo change me
            self.set_isolation_level("SERIALIZABLE")

            cur = self.conn.cursor()

            cur.execute("INSERT INTO packs(id, country, recipent) VALUES(%s, %s, %s)", (id, country, recipent))

            # przyjmujemy zalozenie, ze wysylamy paczki jednego typu
            for sweet in sweets_list:
                found_sweet = False

                cur.execute("SELECT has_left FROM sweets WHERE name = %s;", (sweet["name"],))
                sweets_in_magazine = cur.fetchone()[0]

                # Mamy wlasciwy slodycz
                if sweets_in_magazine >= sweet["number"]:
                    cur.execute("INSERT INTO pack_sweet(pack_id, sweet, number) values (%s, %s, %s)", (id, sweet["name"], sweet["number"]))
                    cur.execute("UPDATE sweets SET has_left = has_left - %s WHERE name = %s", (sweet["number"], sweet["name"]))
                    continue

                # Szukamy podobnego slodycza
                cur.execute("SELECT sweet_2 FROM sweet_resemblance WHERE sweet_1 = %s ORDER BY resemblance DESC", (sweet["name"],))
                similar_sweets = cur.fetchall()

                for similar_sweet in similar_sweets:
                    cur.execute("SELECT has_left FROM sweets WHERE name = %s", (similar_sweet,))
                    similar_sweet_in_magazine = cur.fetchone()[0]
                    if similar_sweet_in_magazine > sweet["number"]:
                        found_sweet = True
                        cur.execute("INSERT INTO pack_sweet(pack_id, sweet, number) values (%s, %s, %s)", (id, similar_sweet, sweet["number"]))
                        cur.execute("UPDATE sweets SET has_left = has_left - %s WHERE name = %s", (sweet["number"], similar_sweet))
                        break

                if not found_sweet:
                    print("CANT FIND SWEETS")
                    self.conn.rollback()
                    return "fail"

            self.conn.commit()
            print("SUCCESS")
            return "success"

        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            return "retry"


class DataGenerator:
    def __init__(self):
        self.sweets_1 = ["chocolate", "lollypop", "biscuit", "cake"]
        self.sweets_2 = []
        for _ in range (100):
            self.sweets_2.append(''.join(random.choices(string.ascii_uppercase, k=10)))
        self.countries = ["Poland", "Germany", "France", "UK", "USA", "Norway", "Sweden"]
        self.names = ["Damian", "John", "Steve", "Mary", "Anne", "Jane"]

    def get_sweets_1(self):
        sweets_list = []
        sweets_list.append(('chocolate', 1000))
        sweets_list.append(('lollypop', 300))
        sweets_list.append(('biscuit', 1000))
        sweets_list.append(('cake', 300))
        return sweets_list
    
    def get_sweets_2(self):
        sweet_list = []
        for sweet in self.sweets_2:
            sweet_list.append((sweet, random.randrange(0, 1000)))
        return sweet_list

    def get_resemblance_1(self):
        resemblane_list = []
        resemblane_list.append(('chocolate', 'lollypop', 0.5))
        resemblane_list.append(('lollypop', 'chocolate', 0.5))
        resemblane_list.append(('biscuit', 'cake', 0.5))
        resemblane_list.append(('cake', 'biscuit', 0.5))
        return resemblane_list

    def get_resemblance_2(self):
        resemblane_list = []
        for _ in range (1000):
            sweet_1 = random.choice(self.sweets_2)
            sweet_2 = random.choice(self.sweets_2)
            if sweet_1 == sweet_2:
                continue
            resemblane_list.append((sweet_1, sweet_2, random.uniform(0, 1)))
        return resemblane_list

    # def fill_sweets_queue(self):
    #     for i in range (1, 1000):
    #         q.put({"id": i, "number": random.randrange(50), "sweet": random.choice(self.sweets)})

    def fill_sweets_queue_1(self):
        for i in range (1, 30):
            l = []
            items_on_list = random.randrange(20)
            for _ in range (items_on_list):
                l.append({"number": random.randrange(20), "name": random.choice(self.sweets_1)})
            q.put({"id": i, "country": random.choice(self.countries), "recipient": random.choice(self.names), "list": l})


    def fill_sweets_queue_2(self):
        for i in range (1, 50):
            l = []
            items_on_list = random.randrange(20)
            for _ in range (items_on_list):
                l.append({"number": random.randrange(100), "name": random.choice(self.sweets_2)})
            q.put({"id": i, "country": random.choice(self.countries), "recipient": random.choice(self.names), "list": l})

def worker():
    manager = DBManager()
    while (not q.empty()):
        global in_queue
        with lock:
            in_queue -= 1
            print(in_queue)
        letter = q.get()
        how_many = 0
        while (True):
            how_many += 1
            if how_many >= 10:
                print("MORE THAN 10")
                return
            code = manager.do_transaction_no_retry(letter["id"], letter["country"], letter["recipient"], letter["list"])
            if code == "retry":
                # sleep_time = random.randrange(0, 10)
                # time.sleep(sleep_time)
                continue
            if code == "success" or code == "fail":
                with lock:
                    global successful_packs
                    successful_packs += 1
            break



if __name__ == '__main__':
    global in_queue
    in_queue = 50
    global successful_packs
    successful_packs = 0

    manager = DBManager()
    data_generator = DataGenerator()

    manager.delete_db()
    manager.create_db()
    manager.fill_db(data_generator.get_sweets_2(), data_generator.get_resemblance_2())

    data_generator.fill_sweets_queue_2()

    start_time = time.time()

    # worker()
    threads = []
    for _ in range(20):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("Successfull packs: ", successful_packs)
    print("Time taken: ", (time.time() - start_time))