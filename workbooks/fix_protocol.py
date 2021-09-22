"""A workbook to process fix protocol info."""
# %% codecell
##############################################
import simplefix


# %% codecell
##############################################

message = simplefix.FixMessage()


"""
 message.append_pair(1, "MC435967")
 message.append_pair(54, 1)
 message.append_pair(44, 37.0582)
 """


parser = simplefix.FixParser()
parser.append_buffer(response_from_socket)
message = parser.get_message()
