import json
import os
import vk_api
import threading
import time
import logging

vk_object = vk_api.VkApi(token="access_token")
vk_tools = vk_api.tools.VkTools(vk_object)
logger = logging.getLogger("parser")
logger.setLevel(logging.INFO)

fh = logging.FileHandler("parser.log")
fh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(fh)


def getDialogs():
    data = vk_tools.get_all(method="messages.getConversations",
                            max_count=200)['items']
    ids = list()
    for peer in data:
        ids.append(peer["conversation"]["peer"]["id"])
    return ids


def getHistory(dialog_id, number_task):
    if 2000000000 > int(dialog_id) > 0:
        data = vk_object.method("users.get", {"user_ids": dialog_id})
        title = data[0]['first_name'] + "_" + data[0]["last_name"]
        history = vk_tools.get_all(method="messages.getHistory",
                                   max_count=200,
                                   values={"user_id": dialog_id})
    elif str(dialog_id)[1:] == "-":
        title = vk_object.method("groups.getById", {"group_ids": dialog_id})[0]["name"]
        print(dialog_id)
        history = vk_tools.get_all(method="messages.getHistory",
                                   max_count=200,
                                   values={"group_id": dialog_id})
    elif 2000000000 < int(dialog_id):
        title = vk_object.method("messages.getConversationsById", {"peer_ids": dialog_id})['items'][0]['chat_settings'][
            'title']
        history = vk_tools.get_all(method="messages.getHistory",
                                   max_count=200,
                                   values={"peer_id": dialog_id})

    else:
        return False

    if os.path.exists("users") is False:
        os.mkdir("users")

    matches = ["?", "|", "<", ">"]
    for match in matches:
        if match in title:
            title = title.replace(match, "")
    messages = []
    for message in history['items']:
        messages.append({
            "message_id": message["id"],
            "dialog_id": dialog_id,
            "title": title,
            "forward_msg": message["fwd_messages"],
            "message": message["text"],
            "attach": message["attachments"]
        })

    with open("users/_" + title + "_.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(messages,
                           ensure_ascii=False,
                           indent=2))
        f.close()
        logger.info("Таск {} закончил свою работу".format(number_task))


def main():
    ids = getDialogs()
    i = 1
    for dialog_id in ids:
        logger.info("Создаю и запускаю таск {}".format(i))
        task = threading.Thread(target=getHistory, args=(dialog_id, i))
        task.start()
        i += 1
        time.sleep(1)
        task.join()
    time.sleep(3)
    logger.info("Закончена выгрузка диалогов")


if __name__ == "__main__":
    main()
