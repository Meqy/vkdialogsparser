import json
import os
import vk_api
import threading

vk_object = vk_api.VkApi(token="access_token")
vk_tools = vk_api.tools.VkTools(vk_object)


def getDialogs(returns="all"):
    data = vk_tools.get_all(method="messages.getDialogs",
                            max_count=200)['items']
    ids = list()
    for messages in data:
        if returns == "all":
            if messages['message']['user_id'] and "chat_id" not in messages['message']:
                if vk_object.method("users.get")[0]['id'] == messages["message"]["user_id"]:
                    continue
                ids.append(messages["message"]["user_id"])
            elif "chat_id" in messages['message']:
                ids.append(str(2000000) + str(messages["message"]["chat_id"]))
    for dialog_id in ids:
        task = threading.Thread(target=getHistory,
                                args=(dialog_id,))
        task.start()
        task.join()


def getHistory(dialog_id):
    if int(dialog_id) < 2000000000:
        data = vk_object.method("users.get", {"peer_id": dialog_id})
        title = data[0]['first_name'] + data[0]["last_name"]
    elif int(dialog_id) < 0:
        title = vk_object.method("groups.getById", {"peer_id": dialog_id})[0]["name"]

    else:
        title = \
            vk_object.method("messages.getConversationsById", {"peer_ids": dialog_id})['items'][0]['chat_settings'][
                'title']
    matchs = ["?", "|"]
    for match in matchs:
        title = title.replace(match, "")

    history = vk_tools.get_all(method="messages.getHistory",
                               max_count=200,
                               values={"peer_id": dialog_id})

    if os.path.exists("users") is False:
        os.mkdir("users")
    f = open("users/_" + title + "_.json",
             "w",
             encoding="utf-8")
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
    f.write(json.dumps(messages,
                       ensure_ascii=False,
                       indent=2))
    f.close()


if __name__ == "__main__":
    getDialogs("all")
