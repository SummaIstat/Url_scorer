import json
from urllib.request import urlopen
import logging
import sys
import os
from datetime import datetime
import tldextract

# logging configuration
logger = logging.getLogger('Url_scorerLogger')
logger.setLevel(logging.DEBUG)
logFormatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)
now = datetime.now()
dateTime = now.strftime("%Y-%m-%d_%H_%M_%S")
LOG_FILE_NAME = "Url_scorer_" + dateTime + ".log"
fileHandler = logging.FileHandler(LOG_FILE_NAME)
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)


FIRMS_INFO_FILE = ""
SOLR_IP_ADDRESS = ""
SOLR_PORT_NUMBER = ""
SOLR_CORE_NAME = ""
SOLR_MAX_DOCS = ""
OUTPUT_FILE_FOLDER = ""
LOG_LEVEL = ""


def load_external_configuration(argv):
    global FIRMS_INFO_FILE
    global SOLR_IP_ADDRESS
    global SOLR_PORT_NUMBER
    global SOLR_CORE_NAME
    global SOLR_MAX_DOCS
    global OUTPUT_FILE_FOLDER
    global LOG_LEVEL

    #if len(argv) != 1: # no args provided in the command line call
    #    sys.exit("Configuration file invalid or not provided ! \nUSAGE: Url_scorer my_path/config.cfg")

    #configFile = sys.argv[1]
    configFile = "config.cfg"
    if not os.path.isfile(configFile):
        logger.error("No \"config.cfg\" configuration file found in the program directory !")
        raise FileNotFoundError("No \"config.cfg\" configuration file found in the program directory !")



    external_settings = dict()
    with open(configFile, "rt") as f:
        for line in f.readlines():
            if not line.startswith("#"):
                tokens = line.split("=")
                if len(tokens) == 2:
                    external_settings[tokens[0]] = tokens[1]

    FIRMS_INFO_FILE = str(external_settings.get("FIRMS_INFO_FILE", "")).rstrip()
    if not os.path.isfile(FIRMS_INFO_FILE):
        logger.error("Invalid FIRMS_INFO_FILE parameter !")
        raise FileNotFoundError("Invalid FIRMS_INFO_FILE parameter !")

    SOLR_IP_ADDRESS = str(external_settings.get("SOLR_IP_ADDRESS", "")).rstrip()
    SOLR_PORT_NUMBER = str(external_settings.get("SOLR_PORT_NUMBER", "")).rstrip()
    SOLR_CORE_NAME = str(external_settings.get("SOLR_CORE_NAME", "")).rstrip()
    SOLR_MAX_DOCS = str(external_settings.get("SOLR_MAX_DOCS", "")).rstrip()

    LOG_LEVEL = str(external_settings.get("LOG_LEVEL", "INFO")).rstrip()
    consoleHandler.setLevel(LOG_LEVEL)
    fileHandler.setLevel(LOG_LEVEL)


def loadFirmsInfo(firms_info_file):
    firmList = []

    with open(FIRMS_INFO_FILE, "rt") as f:
        for line in f.readlines():
            tokens = line.split("\t")
            if len(tokens) == 11:
                myDict = {}
                myDict["firmId"] = tokens[0].rstrip()
                myDict["vat_code"] = tokens[1].rstrip()
                myDict["name"] = tokens[2].rstrip()
                myDict["address"] = tokens[3].rstrip()
                myDict["zip"] = tokens[4].rstrip()
                myDict["municipality"] = tokens[5].rstrip()
                myDict["province"] = tokens[6].rstrip()
                myDict["province_abbreviation"] = tokens[7].rstrip()
                myDict["region"] = tokens[8].rstrip()
                myDict["certified_mail"] = tokens[9].rstrip()
                myDict["tels"] = tokens[10].rstrip()
                firmList.append(myDict)
            else:
                logger.warning("the firm having id=" + tokens[0] + " is malformed and will not be considered !")

    return firmList


def main(argv):
    logger.info("***************************************")
    logger.info("**********   Url_scorer   *************")
    logger.info("***************************************\n\n")

    now = datetime.now()
    dateTime = now.strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Starting datetime: " + dateTime)

    load_external_configuration(argv)
    firmsInfo = loadFirmsInfo(FIRMS_INFO_FILE)[1:]  # ignora l'header del file
    outputFile = getOutputFile()

    with open(outputFile, 'a+', encoding='utf-8') as f:
        f.writelines(
            "FIRM_ID" + "\t" +
            "FIRM_NAME" + "\t" +
            "LINK_POS" + "\t" +
            "URL" + "\t" +
            "SIMPLE_URL" + "\t" +
            "NAME_IN_URL" + "\t" +
            "DOMAIN_IN_PEC1" + "\t" +
            "DOMAIN_IN_PEC2" + "\t" +
            "TEL" + "\t" +
            "VAT" + "\t" +
            "MUN" + "\t" +
            "PROV" + "\t" +
            "ZIP" + "\n")
        f.flush()
        num_of_firms = len(firmsInfo)
        for i, firm in enumerate(firmsInfo):
            # carico da Solr i documenti delle home pages per questo firmId
            logger.info("firm " + str(i+1) + " / " + str(num_of_firms))
            docs = get_docs_by_firm_id(firm["firmId"])
            for doc in docs:
                SIMPLE_URL = get_simple_url(doc["url"])
                NAME_IN_URL = is_subname_in_url(firm["name"], doc["url"])
                extFound = tldextract.extract(doc["url"])
                domainFound = extFound.domain + "." + extFound.suffix
                DOMAIN_IN_PEC1 = get_domain_in_pec1(extFound.domain, firm["certified_mail"])
                DOMAIN_IN_PEC2 = get_domain_in_pec2(domainFound, firm["certified_mail"])

                global page_text
                page_text = str(doc["pageBody"]) + " " + str(doc["titolo"]) + " " + str(doc["metatagDescription"]) + " " + str(doc["metatagKeywords"])
                page_text = page_text.lower()

                TEL = is_tel_in_text(firm["tels"])
                VAT = is_vat_in_text(firm["vat_code"])
                MUN = is_mun_in_text(firm["municipality"])
                PROV = is_prov_in_text(firm["province"], firm["province_abbreviation"])
                ZIP = is_zip_in_text(firm["zip"])

                f.writelines(
                    str(firm["firmId"]) + "\t" +
                    firm["name"] + "\t" +
                    str(doc["linkPosition"]) + "\t" +
                    str(doc["url"]) + "\t" +
                    str(SIMPLE_URL) + "\t" +
                    str(NAME_IN_URL) + "\t" +
                    str(DOMAIN_IN_PEC1) + "\t" +
                    str(DOMAIN_IN_PEC2) + "\t" +
                    str(TEL) + "\t" +
                    str(VAT) + "\t" +
                    str(MUN) + "\t" +
                    str(PROV) + "\t" +
                    str(ZIP) + "\n")
                f.flush()


def is_vat_in_text(vat):
    if vat.lower() in page_text:
        return 1
    return 0


def is_mun_in_text(municipality):
    if municipality.lower() in page_text:
        return 1
    return 0


def is_prov_in_text(province, province_abbreviation):
    if (province.lower() in page_text) or (" " + province_abbreviation + " ".lower() in page_text) or ("(" + province_abbreviation + ")".lower() in page_text):
        return 1
    return 0


def is_zip_in_text(zip_code):
    if zip_code.lower() in page_text:
        return 1
    return 0


def is_tel_in_text(tels):
    result = 0
    if tels == None or tels.strip() == "":
        return result
    else:
        tels_list = tels.split(" ")
        for tel in tels_list:
            splitted = tel.split("/")
            if len(splitted) == 2:
                prefix = splitted[0]
                num = splitted[1]
                combinations = []
                combinations.append(prefix+num)
                combinations.append(prefix + " " + num)
                combinations.append(prefix + "-" + num)
                combinations.append(prefix + " - " + num)
                combinations.append(prefix + "/" + num)
                combinations.append(prefix + " / " + num)
                for num in combinations:
                    if num in page_text:
                        result = 1
                        return result
    return result


def get_simple_url(url):
    result = "0"
    extracted = tldextract.extract(url)
    domain_name = extracted.domain + "." + extracted.suffix
    tokens = url.split(domain_name)
    if len(tokens) == 2:
        if len(tokens[1]) <= 1:
            result = "1"
    return result


def is_subname_in_url(firmName, url):
    ''' Se almeno un token nel nome azienda è contenuto nel dominio dell'url ritorna True '''
    result = 0
    firmName = firmName.replace(".","")
    firmName = firmName.replace(",", " ")
    firmName = firmName.replace("'", " ")
    firmName = firmName.replace("-", " ")
    firmName = firmName.replace('"', ' ')
    tokens = firmName.split(" ")
    extracted = tldextract.extract(url)
    domain_name = extracted.domain + "." + extracted.suffix
    for token in tokens:
        if (len(token) > 2) and (token.lower() in domain_name.lower()):
            result = 1
            break
    return result


def get_domain_in_pec1(domain_no_ext, pec):
    # dominio presente nella PEC prima della @
    result = "0"
    pec = pec.lower()
    domain_no_ext = domain_no_ext.lower()
    if len(pec) != 0:
        tokens = pec.split("@")
        if len(tokens) == 2:
            if domain_no_ext in tokens[0]:
                result = "1"
    return result


def get_domain_in_pec2(domain_with_ext, pec):
    # dominio presente nella PEC dopo la @
    result = "0"
    pec = pec.lower()
    domain_with_ext = domain_with_ext.lower()
    if len(pec) != 0:
        tokens = pec.split("@")
        if len(tokens) == 2:
            if domain_with_ext in tokens[1]:
                result = "1"
    return result


def getOutputFile():
    now = datetime.now()
    dateTime = now.strftime("%Y-%m-%d_%H_%M_%S")
    outputFileName = "link_scores_" + dateTime + ".csv"
    outputFile = outputFileName
    return outputFile


def get_partial_solr_query_url(ipAddress, portNumber, coreName, fieldList, filterQuery=""):
    queryUrl = "http://" + \
               ipAddress + \
               ":" + \
               portNumber + \
               "/solr/" + \
               coreName + \
               "/select?fl=" + \
               '%2C'.join(fieldList) +\
               filterQuery + \
               "&q="
    return queryUrl


def get_docs_by_firm_id(firmId):
    result = []
    fieldList = ["firmId","linkPosition", "url", "titolo", "metatagDescription", "metatagKeywords",
                 "imgsrc", "links", "ahref", "inputvalue", "inputname", "pageBody"]
    partialSolrQueryUrl = get_partial_solr_query_url(SOLR_IP_ADDRESS, SOLR_PORT_NUMBER, SOLR_CORE_NAME, fieldList)
    url = partialSolrQueryUrl + "firmId%3A" + str(firmId) + "&q.op=OR" + "&rows=" + SOLR_MAX_DOCS + "&wt=json"
    connection = urlopen(url)
    response = json.load(connection)
    for node in response['response']['docs']:
        result.append(node)
    return result


if __name__ == "__main__":
    main(sys.argv[1:])