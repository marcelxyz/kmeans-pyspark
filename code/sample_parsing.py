class Parser:

	# Checks for the presence of all the specified fields in an XML object; returns true if they're all found, false if not
	@staticmethod
	def is_valid(string, fields):
		for field in fields:
			if Parser.get_value_for_field(string, field) == None : return false
		return true

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

	fields = ["Reputation", "Age", "Views", "UpVotes", "DownVotes", "CreationDate", "LastAccessDate", "AboutMe"]

	def __init__(self, reputation, age, views, up_votes, down_votes, creation_date, last_access_date, about_me):
		self.reputation = reputation
		self.age = age
		self.views = views
		self.up_votes = up_votes
		self.down_votes = down_votes
		self.creation_date = creation_date
		self.last_access_date = last_access_date
		self.about_me = about_me

	def __init__(self, string):
		self.reputation = Parser().get_value_for_field(string, "Reputation")
		self.reputation = Parser().get_value_for_field(string, "Age")
		self.reputation = Parser().get_value_for_field(string, "Views")
		self.reputation = Parser().get_value_for_field(string, "UpVotes")
		self.reputation = Parser().get_value_for_field(string, "DownVotes")
		self.reputation = Parser().get_value_for_field(string, "CreationDate")
		self.reputation = Parser().get_value_for_field(string, "LastAccessDate")
		self.reputation = Parser().get_value_for_field(string, "AboutMe")