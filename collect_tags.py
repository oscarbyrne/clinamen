from optparse import OptionParser
from collections import defaultdict
from time import time
import contextlib

import leveldb


def get_options():
    parser = OptionParser()
    parser.add_option(
        '-l', '--labels',
        dest='labels',
        help="Image labels (expect labels.csv file)", metavar="FILENAME"
    )
    parser.add_option(
        '-d', '--dictionary',
        dest='dict',
        help="Label dictionary (expect dict.csv file)", metavar="FILENAME"
    )
    parser.add_option(
        '-m', '--metadata',
        dest='meta',
        help="Image metadata (expect images.csv)", metavar="FILENAME"
    )
    parser.add_option(
        '-o', '--outfile',
        dest='outfile',
        help="Output database name", metavar="DATABASE_NAME"
    )
    options, args = parser.parse_args()
    if not all([options.labels, options.dict, options.meta, options.outfile]):
        parser.print_help()
        exit(1)
    return options

@contextlib.contextmanager
def load_tags(path):
    tags = {}
    with open(path, 'r') as f:
        for line in f:
            tag, value = line.rstrip().split('","')
            tag, value = tag[1:], value[:-1] # remove double quotes
            tags[tag] = value
    yield tags
    tags.clear()

@contextlib.contextmanager
def load_urls(path):
    urls = {}
    with open(path, 'r') as f:
        next(f) # skip first line (expect column headings)
        for line in f:
            fields = line.rstrip().split(',')
            image_id, url = fields[0], fields[2]
            urls[image_id] = url
    yield urls
    urls.clear()

def collect_label_mapping(labels_file, tags, urls, min_confidence=0.5):
    tag_to_img = defaultdict(set)
    with open(labels_file, 'r') as f:
        next(f) # skip first line (expect column headings)
        for line in f:
            img_id, _, tag_id, confidence = line.rstrip().split(',')
            if float(confidence) >= min_confidence:
                url = urls[img_id]
                tag = tags[tag_id]
                tag_to_img[tag].add(url)
    return tag_to_img

def write_to_file(tag_to_img, outfile):
    leveldb.DestroyDB(outfile)
    db = leveldb.LevelDB(outfile, create_if_missing=True)
    for tag, urls in tag_to_img.iteritems():
        db.Put(tag, ",".join(urls))

def main():
    start_time = time()
    options = get_options()
    print "Collating data..."
    with load_tags(options.dict) as tags, load_urls(options.meta) as urls:
        tag_to_img = collect_label_mapping(options.labels, tags, urls)
    print "Writing to file..."
    write_to_file(tag_to_img, options.outfile)
    print("Complete! Total execution time: {} seconds".format(time() - start_time))

if __name__=='__main__':
    main()
    
