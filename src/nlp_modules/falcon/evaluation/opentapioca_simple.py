# -*- coding: utf-8 -*-

import requests
import csv
import evaluation



def isLineEmpty(line):
    return len(line) == 0

def read_dataset(filename):
    f = open(filename, 'r',encoding='utf-8')
    rows=f.readlines()
    ans = []
    for q in rows:
        q = q.rstrip('\n')
        line = q.split("\t")
        if 'R' in line[1]:
            line[1].replace('R','P')
        if not isLineEmpty(line):
            line[3]=line[3][0].lower()+line[3][1:]
            ans.append([line[3],[line[0]],[line[1]]])
        # if(len(line)!=4):
        # 	print(q," has more elements")
    f.close()
    return ans

def open_tapioca_call(text):
    # print(text)
    text = text.replace('?', '')

    headers = {
        'Accept': 'application/json',
    }
    url = "https://opentapioca.org/api/annotate"
    payload = 'query='

    data = text.split(" ");
    for s in data:
        payload = payload + s
        payload += '+'
    payload += '%3F'
    payload = payload.encode("utf-8")
    response = requests.request("POST", url, data=payload, headers=headers)
    return response.json()


def evaluate(annotations, raw):
    correctRelations = 0
    wrongRelations = 0
    correctEntities = 0
    wrongEntities = 0
    p_entity = 0
    r_entity = 0
    p_relation = 0
    r_relation = 0
    entities = []

    for annotation in annotations:
        if annotation['best_qid'] is not None:
            entities.append(annotation['best_qid'])
        else:
            if len(annotation['tags']) != 0:
                tags = sorted(annotation['tags'], key=lambda i: i['rank'], reverse=True)
                entities.append(tags[0]['id'])

    true_entity = raw[1]
    numberSystemEntities = len(raw[1])
    # print(true_entity, entities)
    for e in true_entity:
        if e in entities:
            correctEntities = correctEntities + 1
        else:
            wrongEntities = wrongEntities + 1
    intersection = set(true_entity).intersection(entities)
    if len(entities) != 0:
        p_entity = len(intersection) / len(entities)
    r_entity = len(intersection) / numberSystemEntities


    return [correctEntities, wrongEntities, p_entity, r_entity, entities]


if __name__ == "__main__":

    result = []
    result.append(["Question", "Gold Standard", "System", "P", "R"])
    #questions = evaluation.read_lcquad_2()
    questions= read_dataset('../datasets/simplequestions.txt')
    correct = 0
    wrong = 0
    i = 0
    for question in questions:
        output = open_tapioca_call(question[0])
        c, w, p, r, entities = evaluate(output['annotations'], question)
        correct += c
        wrong += w
        # print(c)
        result.append([question[0], question[1], entities, p, r])
        print(str(i) + "#####" + str((correct * 100) / (correct + wrong)))
        i = i + 1
    print("total correct entities: ", correct)
    print("Total wrong entities: ", wrong)
    print("P:")
    print((correct * 100) / (correct + wrong))
    with open('../datasets/results/final/results_simple_entities_OpenTapioca.csv', mode='w', newline='', encoding='utf-8') as results_file:
        writer = csv.writer(results_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(result)



