import asyncio
import json
import httpx
import websockets
from fastapi import FastAPI
import operator
from itertools import islice

app = FastAPI()

OKX_REST_URL = "https://www.okx.com/api/v5/public/instruments"
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
MAX_PAIRS_PER_CONNECTION = 30  # Ограничение OKX

trading_pairs = []  # Здесь будут храниться торговые пары
order_books = dict()


async def fetch_trading_pairs():
    """Получаем список спотовых пар с USDT"""
    global trading_pairs
    async with httpx.AsyncClient() as client:
        response = await client.get(OKX_REST_URL, params={"instType": "SPOT"})
        data = response.json()

        if data.get("code") != "0":
            raise ValueError(f"Ошибка OKX API: {data.get('msg', 'Неизвестная ошибка')}")

        # Фильтруем только пары, заканчивающиеся на '-USDT'
        trading_pairs = [item["instId"] for item in data.get("data", []) if item["instId"].endswith("-USDT")]

        print(f"[INFO] Загружено {len(trading_pairs)} пар с USDT")


def print_ob(s, obs: dict, levels=5) -> None:
    print("\n", s)
    print("\n".join([f"{r[0]} {r[1]} {r[3]}" for r in sorted(obs['asks'], key=operator.itemgetter(0), reverse=True)[-1*levels:]]))
    print("-")
    print("\n".join([f"{r[0]} {r[1]} {r[3]}" for r in sorted(obs['bids'], key=operator.itemgetter(0), reverse=True)[:levels]]))


def snapshot(s, data, ob: dict) -> dict:
    ob[s] = {"asks": [], "bids": [], 'seqId': data[0].get("seqId")}
    ob[s]["asks"] = data[0]["asks"]
    ob[s]["bids"] = data[0]["bids"]

    print_ob(s, ob[s])
    return ob


def update(s, data, ob: dict) -> dict:
    prev_seq_id = data[0].get("prevSeqId")
    if prev_seq_id != ob[s]['seqId']:
        raise Exception(f"{s} prevSeqId is not eq seqId")

    for k in ["asks", "bids"]:
        for v in data[0][k]:
            price = v[0]
            vol = v[1]
            idx = next((idx for idx, val in enumerate(ob[s][k]) if price == val[0]), None)

            if idx is None:
                ob[s][k].append(v)
            elif vol == "0":
                del ob[s][k][idx]
            elif ob[s][k][idx][1] != vol or ob[s][k][idx][3] != v[3]:
                ob[s][k][idx] = v

    ob[s]['seqId'] = data[0].get("seqId")

    print_ob(s, ob[s])
    return ob


async def connect_to_websocket(pairs):
    """ Подключение к WebSocket OKX с подпиской на группу стаканов """
    global order_books
    while True:
        try:
            async with websockets.connect(OKX_WS_URL, ping_interval=20) as ws:
                await subscribe_to_order_books(ws, pairs)

                async for message in ws:
                    data = json.loads(message)
                    inst_id = data["arg"].get("instId")

                    try:
                        if data.get("action") == "snapshot":
                            order_books = snapshot(inst_id, data.get('data'), order_books)
                        elif data.get("action") == "update":
                            order_books = update(inst_id, data.get('data'), order_books)
                    except Exception as e:
                        print(f"[ERROR] Ошибка обработки {inst_id}: {e}")
                        del order_books[inst_id]

                        await resubscribe(ws, inst_id)

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"[ERROR] WebSocket отключился: {e}. Переподключение через 5 сек...")
        except Exception as e:
            print(f"[ERROR] Неожиданная ошибка WebSocket: {e}")

        await asyncio.sleep(5)
        order_books = dict()


async def subscribe_to_order_books(ws, pairs):
    """ Подписка на стаканы для указанных торговых пар """
    channels = [{"channel": "books", "instId": pair} for pair in pairs]
    subscription_msg = {"op": "subscribe", "args": channels}

    await ws.send(json.dumps(subscription_msg))
    print(f"[INFO] Подписались на {len(pairs)} пар в одном WebSocket")


async def resubscribe(ws, inst_id):
    """Отписка и повторная подписка на стакан"""
    unsubscribe_msg = {"op": "unsubscribe", "args": [{"channel": "books", "instId": inst_id}]}
    subscribe_msg = {"op": "subscribe", "args": [{"channel": "books", "instId": inst_id}]}

    await ws.send(json.dumps(unsubscribe_msg))
    print(f"[INFO] Отписались от стакана {inst_id}")

    await asyncio.sleep(1)

    await ws.send(json.dumps(subscribe_msg))
    print(f"[INFO] Подписались заново на стакан {inst_id}")


def split_pairs(trading_pairs, chunk_size):
    """ Разбивает список пар на части по chunk_size элементов """
    it = iter(trading_pairs)
    return iter(lambda: list(islice(it, chunk_size)), [])


@app.get("/orderbook/{instrument_id}")
async def get_order_book(instrument_id: str):
    """ API-эндпоинт для получения текущего стакана по инструменту """
    return order_books.get(instrument_id, {"message": "Нет данных"})


@app.on_event("startup")
async def startup_event():
    # Загружаем торговые пары и запускаем несколько WebSocket соединений
    await fetch_trading_pairs()

    # Разбиваем список пар на группы по 30 инструментов
    pair_batches = list(split_pairs(trading_pairs, MAX_PAIRS_PER_CONNECTION))

    # Запускаем каждую группу в отдельном WebSocket соединении
    for batch in pair_batches:
        asyncio.create_task(connect_to_websocket(batch))






