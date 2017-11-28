# Added sample comment to test new gitflow.

class Parser:

    # Checks for the presence of all the specified fields in an XML object; returns true if they're all found, false if not
    @staticmethod
    def is_valid(string, fields):
        for field in fields:
            if Parser.get_value_for_field(string, field) == None : return False
        return True

    # Checks for the presence of a field in an XML object; returns the value for that field if found, None if not found
    @staticmethod
    def get_value_for_field(string, field_name):
        beg = " " + field_name + "=\""
        end = "\" "
        beg_i = string.find(beg)
        if (beg_i != -1) :
            substring = string[beg_i+len(beg):len(string)]
            end_i = substring.find(end)
            if(end_i != -1):
                substring = substring[0:end_i]
                return substring

class User:

    fields = ["Id", "Reputation", "Age", "Views", "UpVotes", "DownVotes", "CreationDate", "LastAccessDate", "AboutMe"]

    def __init__(self, id, reputation, age, views, up_votes, down_votes, creation_date, last_access_date, about_me):
        self.id = id
        self.reputation = reputation
        self.age = age
        self.views = views
        self.up_votes = up_votes
        self.down_votes = down_votes
        self.creation_date = creation_date
        self.last_access_date = last_access_date
        self.about_me = about_me

    def __init__(self, string):
        self.id = Parser().get_value_for_field(string, "Id")
        self.reputation = Parser().get_value_for_field(string, "Reputation")
        self.age = Parser().get_value_for_field(string, "Age")
        self.views = Parser().get_value_for_field(string, "Views")
        self.up_votes = Parser().get_value_for_field(string, "UpVotes")
        self.down_votes = Parser().get_value_for_field(string, "DownVotes")
        self.creation_date = Parser().get_value_for_field(string, "CreationDate")
        self.last_access_date = Parser().get_value_for_field(string, "LastAccessDate")
        self.about_me = Parser().get_value_for_field(string, "AboutMe")

class Badge:

    fields = ["UserId", "Name", "Date"]

    def __init__(self, string):
        self.user_id = Parser().get_value_for_field(string, "UserId")
        self.name = Parser().get_value_for_field(string, "Name")
        self.date = Parser().get_value_for_field(string, "Date")

# user_row = "<row Id=\"16193\" Reputation=\"1858\" CreationDate=\"2008-09-17T15:17:49.837\" DisplayName=\"dhiller\" LastAccessDate=\"2014-01-17T13:30:56.400\" WebsiteUrl=\"http://www.dhiller.de\" Location=\"Muenster, Germany\" AboutMe=\"&lt;p&gt;Java software developer&lt;/p&gt;&#xA;\" Views=\"225\" UpVotes=\"225\" DownVotes=\"3\" Age=\"42\" AccountId=\"8891\" />"
# badge_row = "<row Id=\"131133\" UserId=\"10410\" Name=\"Supporter\" Date=\"2008-09-26T00:47:34.057\" />"
