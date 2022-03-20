from peewee import *
import datetime,os
import requests
import time
from progress.bar import Bar
from useragent import UserAgents
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
db = SqliteDatabase('CarDB.db')


class BaseModel(Model):
    class Meta:
        database = db


class Marka(BaseModel):
    Ad = TextField(index=True, unique=True)
    Link = TextField(unique=True)
    imgSrc = TextField(null=True)
    created_date = DateTimeField(default=datetime.datetime.now)


class Model(BaseModel):
    Marka = ForeignKeyField(Marka, backref='models')
    Ad = TextField(index=True)
    Link = TextField(unique=True)
    imgSrc = TextField(null=True)
    created_date = DateTimeField(default=datetime.datetime.now)
    sinifAktarim = BooleanField(default=False)


class Sinif(BaseModel):
    Marka = ForeignKeyField(Marka, backref='sinifMarka')
    Model = ForeignKeyField(Model, backref='sinifModel')
    Ad = TextField(null=True)
    Link = TextField(unique=True)
    imgSrc = TextField(null=True)
    baslangicYil = IntegerField(null=True)
    bitisYil = IntegerField(null=True)
    created_date = DateTimeField(default=datetime.datetime.now)
    nesilAktarim = BooleanField(default=False)


class Nesil(BaseModel):
    Marka = ForeignKeyField(Marka, backref='nesilMarka')
    Model = ForeignKeyField(Model, backref='nesilModel')
    Sinif = ForeignKeyField(Sinif, backref='nesilSinif')
    Ad = TextField(null=True)
    UzunAd = TextField(null=True, index=True)
    Link = TextField(unique=True)
    baslangicYil = IntegerField(null=True)
    bitisYil = IntegerField(null=True)
    created_date = DateTimeField(default=datetime.datetime.now)


class TeknikOzellikler(BaseModel):
    Nesil = ForeignKeyField(Nesil, backref='technics')
    Header = TextField(null=True)
    HeaderID = TextField(null=True)
    Title = TextField(null=True)
    Value = TextField(null=True)
    created_date = DateTimeField(default=datetime.datetime.now)


basePath = "https://www.auto-data.net"


def renewIPadress(adress):
    try:
        res = requests.get(
            adress, headers={"User-Agent": UserAgents.generateAgent()}, timeout=30)
        if (res.status_code == 200):
            soup = bs(res.content, "html.parser")
        else:
            res.close()
            renewIPadress(adress)
        res.close()
        return soup
    except requests.ConnectionError as e:
        print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
        print(str(e))
        renewIPadress(adress)
    except requests.Timeout as e:
        print("OOPS!! Timeout Error")
        print(str(e))
        renewIPadress(adress)
    except requests.RequestException as e:
        print("OOPS!! General Error")
        print(str(e))
        renewIPadress(adress)
    except KeyboardInterrupt:
        print("Someone closed the program")
    except e:
        print(str(e))
        time.sleep(5)
        renewIPadress(adress)


def getTechDetail(nesil):
    soup = renewIPadress(nesil.Link)
    h1 = soup.find('h1', attrs={"class": "top"})
    if h1:
        nesil.UzunAd = h1.text.strip()
        nesil.save()
    table = soup.find('table', attrs={"class", "cardetailsout"})
    tbody = None
    if table:
        tbody = table.findAll('tr')
    if tbody:
        header = None
        headerID = None
        for tr in tbody:
            title = None
            value = None
            if tr.attrs != {} and tr["class"][0] == "no":
                header = tr.text.strip()
                headerID = tr.find("strong")["id"] if tr.find(
                    "strong") else None
                continue
            else:
                [x.extract()
                 for x in tr.findAll('span', attrs={"class", "val2"})]
                title = tr.find("th").text.strip()
                value = tr.find("td").text.strip() if tr.find("td") else ''
            td, created = TeknikOzellikler.get_or_create(
                Nesil=nesil,
                Header=header,
                HeaderID=headerID,
                Title=title,
                Value=value
            )


def getNesil(sinif):
    soup = renewIPadress(sinif.Link)
    # soup = bs(r.content)
    brandLinks = soup.findAll("tr", attrs={"class": "i"})
    for bl in brandLinks:
        a = bl.find("a")
        link=basePath+a["href"]
        if Nesil.get_or_none(Link=link) is None:
            bbt = bl.find("span", attrs={"class", "end"}) if bl.find("span", attrs={
                "class", "end"}) else bl.find("span", attrs={"class", "cur"})
            basTarih = None
            bitTarih = None
            if bbt:
                bbtVal = bbt.text.strip().split("-")
                basTarih = int(bbtVal[0].strip()) if bbtVal[0].strip() !='' else None
                bitTarih = int(bbtVal[1].strip()) if len(
                    bbtVal) == 2 and bbtVal[1].strip() != '' else None
            nesil = Nesil.create(
                Marka=sinif.Marka,
                Model=sinif.Model,
                Sinif=sinif,
                Ad=a.text.strip(),
                Link=link,
                baslangicYil=basTarih,
                bitisYil=bitTarih,
                UzunAd=None
            )


def getSinif(model):
    soup = renewIPadress(model.Link)
    brandLinks = soup.findAll("tr", attrs={"class": "f"})
    for bl in brandLinks:
        a = bl.find("a", attrs={"class": "position"})
        link=basePath+a["href"]
        if Sinif.get_or_none(Link=link) is None:
            bbt = bl.find("strong", attrs={"class", "end"}) if bl.find("strong", attrs={
                "class", "end"}) else bl.find("strong", attrs={"class", "cur"})
            basTarih = None
            bitTarih = None
            if bbt:
                bbtVal = bbt.text.strip().split("-")
                basTarih = int(bbtVal[0].strip())
                bitTarih = int(bbtVal[1].strip()) if len(
                    bbtVal) == 2 and bbtVal[1].strip() != '' else None
            img = bl.find('img')
            sinif = Sinif.create(
                Marka=model.Marka,
                Model=model,
                Ad=a.text.strip(),
                Link=link,
                imgSrc=basePath+img["src"] if img else None,
                baslangicYil=basTarih,
                bitisYil=bitTarih,
                nesilAktarim=False
            )


def getModel(marka):
    soup = renewIPadress(marka.Link)
    brandLinks = soup.findAll("a", attrs={"class": "modeli"})
    for bl in brandLinks:
        link=basePath+bl["href"]
        if Model.get_or_none(Link=link) is None:
            model = Model.create(
                Marka=marka,
                Ad=bl.text.strip(),
                imgSrc=basePath + bl.find('img')["src"],
                Link=link,
                sinifAktarim=False)


def wrapAndSaveMarka():
    soup = renewIPadress(basePath+"/tr/allbrands")
    brandLinks = soup.findAll("a", attrs={"class": "marki_blok"})
    for bl in brandLinks:
        marka, created = Marka.get_or_create(
            Ad=bl.text.strip(),
            imgSrc=basePath + bl.next["src"],
            Link=basePath+bl["href"])


def start():

    wrapAndSaveMarka()

    allMarka = Marka.select()
    pbar = tqdm(allMarka,total=(len(allMarka)))
    for marka in pbar:        
        pbar.set_description(marka.Ad);
        getModel(marka)
    pbar=None
    allModel = Model.select()
    pbar = tqdm(allModel,total=(len(allModel)))
    for m in pbar:
        pbar.set_description(m.Ad)
        getSinif(m)
        m.sinifAktarim = True
        m.save()
    pbar=None
    allSinif = Sinif.select()
    pbar = tqdm(allSinif,total=(len(allSinif)))
    for m in pbar:
        pbar.set_description(m.Ad)
        getNesil(m)
        m.nesilAktarim = True
        m.save()
    pbar=None
    allNesil = Nesil.select()  
    # bar = Bar('Processing', max=len(allNesil))
    pbar = tqdm(allNesil,total=(len(allNesil)))
    for m in pbar:
        pbar.set_description(str(m.Marka.Ad)+" "+m.Ad);
        if TeknikOzellikler.get_or_none(Nesil=m) is None:
            getTechDetail(m)



db.connect()
db.create_tables([Marka, Model, Nesil, TeknikOzellikler, Sinif])
start()
db.close()
# os.system('shutdown /p /f');
