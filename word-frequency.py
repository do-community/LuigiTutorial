from collections import Counter
import pickle
import requests
import luigi
from bs4 import BeautifulSoup


class GetTopBooks(luigi.Task):
    """
    Get list of the most popular books from Project Gutenberg
    """

    def output(self):
        return luigi.LocalTarget("data/books_list.txt")

    def run(self):
        # Download the contents of the top page
        resp = requests.get("http://www.gutenberg.org/browse/scores/top")

        # Parse the contents of the page
        soup = BeautifulSoup(resp.content, "html.parser")

        # Get the header from the most popular books list
        pageHeader = soup.find_all("h2", string="Top 100 EBooks yesterday")[0]
        listTop = pageHeader.find_next_sibling("ol")

        # Open the Luigi Target file
        with self.output().open("w") as f:
            # For each item in the top books list, extract the
            # URL of the book and convert it into a download link
            for result in listTop.select("li>a"):
                if "/ebooks/" in result["href"]:
                    f.write("http://www.gutenberg.org{link}.txt.utf-8\n"
                        .format(
                            link=result["href"]
                        )
                    )

class DownloadBooks(luigi.Task):
    """
    Download a specified list of books
    """
    FileID = luigi.IntParameter()

    # List of characters to remove from the downloaded books
    REPLACE_LIST = """.,"';_[]:*-"""

    def requires(self):
        return GetTopBooks()

    def output(self):
        return luigi.LocalTarget("data/downloads/{}.txt".format(self.FileID))

    def run(self):
        with self.input().open("r") as i:
            # Open the line specified by the FileID parameter
            URL = i.read().splitlines()[self.FileID]

            with self.output().open("w") as outfile:
                # Download the book contents from Project Gutenberg
                book_downloads = requests.get(URL)
                book_text = book_downloads.text

                # Replace any special characters from the book text
                for char in self.REPLACE_LIST:
                    book_text = book_text.replace(char, " ")

                # Convert the contents of the book to lower case so we can
                # compare works with different cases
                book_text = book_text.lower()
                outfile.write(book_text)

class CountWords(luigi.Task):
    """
    Count the frequency of the most common words from a file
    """

    FileID = luigi.IntParameter()

    def requires(self):
        return DownloadBooks(FileID=self.FileID)

    def output(self):
        return luigi.LocalTarget(
            "data/counts/count_{}.pickle".format(self.FileID),
            format=luigi.format.Nop
        )

    def run(self):
        with self.input().open("r") as i:
            # Get the frequency of words in the loaded book
            word_count = Counter(i.read().split())

            with self.output().open("w") as outfile:
                # Pickle the counter object and write it to the target
                pickle.dump(word_count, outfile)

class GlobalParams(luigi.Config):
    NumberBooks = luigi.IntParameter(default=10)
    NumberTopWords = luigi.IntParameter(default=500)

class TopWords(luigi.Task):
    """
    Aggregate the count results from the different files
    """

    def requires(self):
        requiredInputs = []
        for i in range(GlobalParams().NumberBooks):
            requiredInputs.append(CountWords(FileID=i))
        return requiredInputs

    def output(self):
        return luigi.LocalTarget("data/summary.txt")

    def run(self):
        # Create an empty Counter object for summarising the
        # individual counts
        total_count = Counter()
        for input in self.input():
            with input.open("rb") as infile:
                nextCounter = pickle.load(infile)
                total_count += nextCounter

        with self.output().open("w") as f:
            # Get the Top most common words from the aggregated results
            # and write them to the target file
            for item in total_count.most_common(GlobalParams().NumberTopWords):
                f.write("{0: <15}{1}\n".format(*item))

