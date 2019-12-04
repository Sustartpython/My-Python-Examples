from APSjournal import oneAPSJournal
from ascejournal import oneAsceJournal
from ascebook import oneAsceBook
from aiaabook import oneAiaaBook
from aiaajournal import oneAiaaJournal
from ydylcnbook import oneYdylBook
from ydylcninfo import oneYdylInfo
from pishubook import onePiShuBook
from pishuinfo import onePiShuInfo
from bioonejournal import oneBioOneJournal
from science import oneScienceJournal
from hepjournal import oneHepJournal
from hepengineeringjournal import oneHepEngineeringJournal

MapProvider = {
    'apsjournal': oneAPSJournal,
    'ascejournal': oneAsceJournal,
    'ascebook': oneAsceBook,
    'aiaabook': oneAiaaBook,
    'aiaajournal': oneAiaaJournal,
    'ydylcnbook' : oneYdylBook,
    'ydylcninfo' : oneYdylInfo,
    'pishubook' : onePiShuBook,
    'pishuinfo' : onePiShuInfo,
    'bioonejournal' : oneBioOneJournal,
    'sciencejournal' : oneScienceJournal,
    'hepjournal' : oneHepJournal,
    'hepengineeringjournal' : oneHepEngineeringJournal

}


def Update():
    oneAPSJournal.update()
    # oneAsceBook.update()
    # oneAsceJournal.update()
    # oneAiaaJournal.update()
    # oneAiaaBook.update()
    # oneYdylInfo.update()
    # oneYdylBook.update()
    # oneBioOneJournal.update()
    # oneScienceJournal.update()
    # onePiShuBook.update()
    # onePiShuInfo.update()
    # oneAsceBook.update()
    # oneHepJournal.update()
    # oneHepEngineeringJournal.update()


