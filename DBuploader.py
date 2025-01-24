class DB_con:
    def __init__(self, user, password, url):
        self.user = user
        self.password = password
        self.url = url

    def con_DB(self):
        print("{0}/{1}{2}".format(self.user, self.password, self.url))


a = DB_con(
    "CIN/EDU",
)
a.con_DB()
