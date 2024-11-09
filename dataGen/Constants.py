#

class MySQLValues:
    __host = '127.0.0.1'
    __user = 'root'
    __password = '###'
    __database = 'test_weather'

    def getHost(self):
        return str(self.__host)

    def getUser(self):
        return str(self.__user)

    def getPassword(self):
        return self.__password

    def getDatabaseName(self):
        return self.__database
