import pandas as pd
import spacy
from tqdm import tqdm
from spacy.language import Language

from spacy_language_detection import LanguageDetector

df = pd.read_csv('./output/data/msgs_dataset.csv', encoding='utf-8', delimiter='\t', error_bad_lines=False)
print(df['message'])

def get_lang_detector(nlp, name):
    return LanguageDetector(seed=42)  # We use the seed 42


nlp_model = spacy.load("de_core_news_sm")
Language.factory("language_detector", func=get_lang_detector)
nlp_model.add_pipe('language_detector', last=True)
df['language'] = ''
for index, row in df.iterrows():
    df['message'][index] = str(df['message'][index])
i = 0
for doc in tqdm(nlp_model.pipe(df['message']), total=len(df)):
    df['language'][i] = doc._.language['language']
    i+=1
    # print(doc._.language)
# for index, row in df.iterrows():
#     language = nlp_model(str(row['message']))._.language['language']
#     df['language'][index] = language
print(df)
print(df['language'].value_counts())
df.to_csv(
				'./output/data/msgs_dataset_annotated.csv',
				encoding='utf-8',
				index=False,
				# mode='a',
				sep='\t'
			)