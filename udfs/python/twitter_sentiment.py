from __future__ import absolute_import, division, print_function

import boto
import re
from pig_util import outputSchema

conn = boto.connect_s3("AKIAJRRFNYD7XORA3GSQ", "i9sELVnySKCBfwTlOA1NHbGk2n9zszIPJauAVCRN")
bucket = conn.get_bucket("mortar-example-data")
key = bucket.get_key("ngrams/books/20120701/eng-all/dictionary_150k.txt")

words = [re.split('\\s+', line) for line in key.get_contents_as_string().split('\n')]
words.pop() # get rid of empty string at the end after newline split
# word : frequency
english_words = { word_data[0]:float(word_data[2]) for word_data in words }

positive_words = set([
"addicting", "addictingly", "admirable", "admirably", "admire", "admires", "admiring", "adorable", 
"adorably", "adore", "adored", "adoring", "amaze", "amazed", "amazes", "amazing", 
"angelic", "appeal", "appealed", "appealing", "appealingly", "appeals", "attentive", "attracted", 
"attractive", "awesome", "awesomely", "beautiful", "beautifully", "best", "bliss", 
"bold", "boldly", "boss", "breath-taking", "breathtaking", "calm", "cared", "cares", 
"caring", "celebrate", "celebrated", "celebrating", "charm", "charmed", "charming", "charmingly", 
"cheer", "cheered", "cheerful", "cheerfully", "classic", "colorful", "colorfully", "colourful", 
"colourfully", "comfort", "comfortably", "comforting", "comfortingly", "comfy", "competent", "competently", 
"considerate", "considerately", "cool", "courteous", "courteously", "creative", "creatively", "cute", 
"dapper", "dazzled", "dazzling", "dazzlingly", "delicious", "deliciously", "delight", "delighted", 
"delightful", "delightfully", "dope", "dynamic", "ecstatic", "efficient", "efficiently", "elegant", 
"elegantly", "eloquent", "energetic", "energetically", "engaging", "engagingly", "enjoy", "enjoyed", 
"enjoying", "enticing", "enticingly", "essential", "excellent", "excellently", "exceptional", "excitement", 
"exciting", "excitingly", "exquisite", "exquisitely", "fantastic", "fascinating", "fashionable", "fashionably", 
"fast", "favorite", "favorites", "favourite", "favourites", "fetching", "fine", "flattering", 
"fond", "fondly", "friendly", "fulfilling", "fun", "generous", "generously", "genius", 
"genuine", "glamor", "glamorous", "glamorously", "glamour", "glamourous", "glamourously", "glorious", 
"good", "good-looking", "goodlooking", "gorgeous", "gorgeously", "grace", "graceful", "gracefully", 
"great", "handsome", "happiness", "happy", "healthy", "heartwarming", "heavenly", "helpful", 
"hip", "imaginative", "incredible", "ingenious", "innovative", "inspirational", "inspired", "inspiring", 
"intelligent", "interesting", "invigorating", "irresistible", "irresistibly", "joy", "kawaii", "keen", 
"knowledgeable", "liked", "lively", "love", "loved", "lovely", "loving", "lucky", 
"luscious", "lusciously", "magical", "marvelous", "marvelously", "masterful", "masterfully", "memorable", 
"mmm", "mmmm", "mmmmm", "natural", "neat", "neatly", "nice", 
"nicely", "nifty", "optimistic", "outstanding", "outstandingly", "overjoyed", "pampered", "peace", 
"peaceful", "phenomenal", "pleasant", "pleasantly", "pleasurable", "pleasurably", "plentiful", "polished", 
"positive", "powerful", "powerfully", "prettily", "pretty", "profound", "proud", "proudly", 
"quick", "quickly", "rad", "radiant", "rejoice", "rejoiced", "rejoicing", "remarkable", 
"respectable", "respectably", "respectful", "satisfied", "serenity", "sexily", "sexy", "shiny", 
"skilled", "skillful", "slick", "smooth", "spectacular", "spicy", "splendid", "straightforward", 
"stunning", "stylish", "stylishly", "sublime", "succulent", "super", "superb", "swell", 
"tastily", "tasty", "terrific", "thorough", "thrilled", "thrilling", "tranquil", "tranquility", 
"treat", "unreal", "vivacious", "vivid", "warm", "welcoming", "well-spoken", "win", 
"wonderful", "wonderfully", "wow", "wowed", "wowing", "wows", "yummy",
"magnificent", "precious", 'congrats', 'congratulations', 'bravo'
])

negative_words = set([
"a-hole", "a-holes", "abandoned", "abandoning", "abuse", "abused", "abysmal", "aggressive", 
"agonizing", "agonizingly", "agony", "ahole", "aholes", "alarming", "anger", "angering", 
"angry", "appalled", "appalling", "appalls", "argue", "argued", "arguing", "ashamed", 
"asshole", "assholes", "atrocious", "awful", "awkward", "bad", "badgered", "badgering", 
"banal", "bankrupt", "bastard", "bastards", "belittled", "belligerent", "berated", "bigot", 
"bigoted", "bigots", "bitch", "bland", "bonkers", "boring", "bossed-around", "bothered", 
"bothering", "bothers", "broke", "broken", "broken-hearted", "brokenhearted", "bummed", "calamitous", 
"callous", "cheated", "cheating", "claustrophobic", "clumsy", "colorless", "colourless", "conceited", 
"condescending", "confused", "confuses", "confusing", "corrupt", "coward", "cowardly", "cowards", 
"creeper", "crestfallen", "cringe-worthy", "cringeworthy", "cruel", "cunt", "cunts", "cursed", 
"cynical", "d-bag", "d-bags", "dbag", "dbags", "degrading", "dehumanized", "dehumanizing", 
"delay", "delayed", "deplorable", "depressed", "despicable", "destroyed", "destroying", "destroys", 
"detestable", "dick", "dicks", "died", "dirty", "disappointed", "disappointing", "disappoints", 
"disaster", "disastrous", "disastrously", "disgruntled", "disgusted", "disgusting", "disgustingly", "dismal", 
"disorganized", "disrespectful", "douche", "douchebag", "douchebags", "dour", "dreadful", "dull", 
"dumb", "egocentric", "egotistical", "embarrassing", "enraging", "fail", "failed", "failing", 
"fails", "failure", "fake", "falsehood", "fight", "fighting", "forgettable", "fought", 
"freaked", "freaking", "frustrated", "frustrating", "fubar", "fuck", "fuckers", "fugly", 
"furious", "gaudy", "ghastly", "gloomy", "greed", "greedy", "grief", "grieve", 
"grieved", "grieving", "grouchy", "hassle", "hate", "hated", "hating", "heart-breaking", 
"heart-broken", "heartbreaking", "heartbroken", "hellish", "hellishly", "helpless", "horrendous", "horrible", 
"horribly", "horrific", "horrifically", "humiliated", "humiliating", "hurt", "hurts", "icky", 
"idiot", "idiotic", "ignorant", "ignored", "ill", "immature", "inane", "inattentive", 
"incompetent", "incompetently", "incomplete", "inconsiderate", "incorrect", "indoctrinated", "inelegant", "infuriating", 
"infuriatingly", "insecure", "insignificant", "insufficient", "insult", "insulted", "insulting", "interrupted", 
"jaded", "kill", "lame", "loathsome", "lonely", "lose", "loser", "lost", 
"mad", "mean", "mediocre", "melodramatic", "missing", "mistreated", "moron", "moronic", 
"mother-fucker", "mother-fuckers", "motherfucker", "motherfuckers", "mourn", "mourned", "mugged", "nagging", 
"nasty", "nazi", "nazis", "negative", "neurotic", "nonsense", "noo", "nooo", 
"nooooo", "nut-job", "nut-jobs", "nutjob", "nutjobs", "objectification", "objectified", "objectifying", 
"obscene", "odious", "offended", "oppressive", "over-sensitive", "pain", "painfully", "panic", 
"panicked", "panicking", "paranoid", "pathetic", "pessimistic", "pestered", "pestering", "petty", 
"pissed", "poor", "poorly", "powerless", "prejudiced", "pretentious", "psychopath", "psychopathic", 
"psychopaths", "psychotic", "quarrelling", "quarrelsome", "racist", "rage", "repugnant", "repulsive", 
"resent", "resentful", "resenting", "retarded", "revolting", "ridicule", "ridiculed", "ridicules", 
"robbed", "rude", "sad", "sadistic", "sadness", "scared", "screwed", "self-centered", 
"selfcentered", "selfish", "shambolic", "shameful", "shamefully", "shattered", "shit", "shitty", 
"shoddy", "sickening", "sloppily", "sloppy", "slow", "slowly", "smothered", "snafu", 
"spiteful", "square", "squares", "stereotyped", "stifled", "stressed", "stressful", "stressing", 
"stuck", "stuffy", "stupid", "sub-par", "subpar", "substandard", "suck", "sucks", 
"suffer", "suffering", "suicide", "superficial", "terrible", "terribly", "train-wreck", "trainwreck", 
"ugly", "unappealing", "unattractive", "uncomfortable", "uncomfy", "unengaging", "unengagingly", "unenticing", 
"unenticingly", "unexceptionable", "unfair", "unfashionable", "unfashionably", "unfriendly", "ungraceful", "ungrateful", 
"unhelpful", "unimpressive", "uninspired", "unjust", "unlucky", "unnotable", "unpleasant", "unpleasantly", 
"unsatisfactory", "unsatisfied", "unseemly", "unwelcoming", "upset", "vicious", "vindictive", "weak", 
"wreck", "wrecked", "wrecking", "wrecks", "wtf", "yucky", "brutal", "barbaric", "asinine", "fool", "fools", "foolish", 
"folly", "error", "erred", "erring", "mistake", "weak"
])

intensifier_words = set([
"absolutely", "amazingly", "exceptionally", "fantastically", "fucking", "incredibly", "obscenely", "phenomenally", 
"profoundly", "really", "remarkably", "ridiculously", "so", "spectacularly", "stunningly", "such", 
"totally", "unquestionably", "very"
])

negation_words = set([
"didn't", "don't", "lack", "lacked", "no-one", "nobody", "noone", "not", "wasn't"
])

# english_words = english_words | positive_words | negative_words

non_english_character_pattern = re.compile("[^a-z']")
word_with_punctuation_pattern = re.compile("^[^a-z']*([a-z']+)[^a-z']*$")
whitespace_pattern = re.compile('\\s')

def is_alphabetic(s):
    return not bool(non_english_character_pattern.search(s))

def words(text):
    if text:
        return [word for word in 
                [re.sub(word_with_punctuation_pattern, '\\1', word) 
                 for word in re.split(whitespace_pattern, text.lower())
                ] if is_alphabetic(word)]
    else:
        return None

@outputSchema("relevance: int")
def relevance(text, pattern_str):
    if text:
        pattern = re.compile(pattern_str)
        return len(re.findall(pattern, text))
    else:
        return 0

def sentiment_of_words(words):
    score = 0.0

    if words is None:
        return 0.0

    positive = [i for i, word in enumerate(words) if word in positive_words]
    negative = [i for i, word in enumerate(words) if word in negative_words]

    for idx in positive:
        word_score = 1.0
        num_negations = 0

        i = idx - 1
        while i >= 0:
            if words[i] in intensifier_words:
                word_score += 1
            elif words[i] in negation_words:
                num_negations += 1
            else:
                break
            i -= 1

        score += word_score * ((-0.5 ** num_negations) if num_negations > 0 else 1)

    for idx in negative:
        word_score = -1.0
        num_negations = 0

        i = idx - 1
        while i >= 0:
            if words[i] in intensifier_words:
                word_score += 1
            elif words[i] in negation_words:
                num_negations += 1
            else:
                break
            i -= 1

        score += word_score * ((-0.5 ** num_negations) if num_negations > 0 else 1)

    return score

@outputSchema("sentiment: float")
def sentiment(text):
    return sentiment_of_words(words(text))

desire_pattern = re.compile("i\\swant|i\\sneed|i'd\\slike|i'd\\slove|i\\swish")
opinion_pattern = re.compile("i\\sthink|should|shouldn't")

@outputSchema("desire_flag: int")
def is_desire(text):
    if text:
        return (1 if bool(desire_pattern.search(text.lower())) else 0)
    else:
        return 0

@outputSchema("opinion_flag: int")
def is_opinion(text):
    if text:
        return (1 if bool(opinion_pattern.search(text.lower())) else 0)
    else:
        return 0

@outputSchema("weight: double")
def tweet_weight(relevance, influence, sentiment, desire_flag, opinion_flag):
    return 1.0 * relevance * influence * abs(sentiment) * (2 if desire_flag else 1) * (2 if opinion_flag else 1)

@outputSchema("words: {t: (word: chararray, occurrences: int)}")
def significant_english_word_count(text):
    word_list = words(text)
    return [(w, word_list.count(w)) for w in set(word_list) if (len(w) > 3 and w in english_words)]

@outputSchema("rel_frequency: double")
def rel_word_frequency_to_english(word, frequency):
    english_freq = english_words.get(word, None)
    if english_freq:
        return frequency / english_freq
    else:
        return 0
