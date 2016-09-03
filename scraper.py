# Idea for this script was taken from here: https://www.dataquest.io/blog/apartment-finding-slackbot/

from craigslist import CraigslistForSale
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean
from sqlalchemy.orm import sessionmaker
from dateutil.parser import parse
from slackclient import SlackClient
import time
import settings

engine = create_engine('sqlite:///listings.db', echo=False)

Base = declarative_base()


class Listing(Base):
    """
    A table to store data on craigslist listings.
    """

    __tablename__ = 'listings'

    id = Column(Integer, primary_key=True)
    link = Column(String, unique=True)
    created = Column(DateTime)
    name = Column(String)
    price = Column(Float)
    location = Column(String)
    cl_id = Column(Integer, unique=True)


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def scrape():
    """
    Scrapes craigslist for a certain geographic area, and finds the latest listings.
    :param area:
    :return: A list of results.
    """
    cl = CraigslistForSale(site=settings.CRAIGSLIST_SITE, category=settings.CATEGORY,
                           filters={'max_price': settings.MAX_PRICE, 'min_price': settings.MIN_PRICE,
                                    'has_image': True})

    results = []
    gen = cl.get_results(sort_by='newest', geotagged=True, limit=20)
    while True:
        try:
            result = next(gen)
        except StopIteration:
            break
        except Exception:
            continue
        listing = session.query(Listing).filter_by(cl_id=result["id"]).first()

        # Don't store the listing if it already exists.
        if listing is None:
            if result["where"] != 'Ann Arbor':
                continue

            # Try parsing the price.
            price = 0
            try:
                price = float(result["price"].replace("$", ""))
            except Exception:
                pass

            # Create the listing object.
            listing = Listing(
                link=result["url"],
                created=parse(result["datetime"]),
                name=result["name"],
                price=price,
                location=result["where"],
                cl_id=result["id"],
            )

            # Save the listing so we don't grab it again.
            session.add(listing)
            session.commit()
            results.append(result)

    return results


def do_scrape():
    """
    Runs the craigslist scraper, and posts data to slack.
    """

    # Create a slack client.
    sc = SlackClient(settings.SLACK_TOKEN)

    # Get all the results from craigslist
    results = scrape()

    print("{}: Got {} results".format(time.ctime(), len(results)))

    # Post each result to slack.
    for result in results:
        post_listing_to_slack(sc, result)


def post_listing_to_slack(sc, listing):
    """
    Posts the listing to slack.
    :param sc: A slack client.
    :param listing: A record of the listing.
    """
    desc = "{0} | {1} | {2} | <{3}>".format(listing["datetime"], listing["price"], listing["name"], listing["url"])
    sc.api_call(
        "chat.postMessage", channel=settings.SLACK_CHANNEL, text=desc,
        username='pybot', icon_emoji=':robot_face:'
    )
