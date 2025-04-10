import asyncio
import websockets
from dotenv import load_dotenv
from ocpp.v16 import ChargePoint as CP
from ocpp.v16.enums import RegistrationStatus
from ocpp.v16 import call_result
from ocpp.routing import on
import motor.motor_asyncio
import os
from datetime import datetime

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
db = client.ocpp

class ChargePoint(CP):
    @on('BootNotification')
    async def on_boot_notification(self, charge_point_model, charge_point_vendor, **kwargs):
        await db.heartbeat.insert_one({"id": self.id, "time": datetime.utcnow()})
        return call_result.BootNotificationPayload(
            current_time=datetime.utcnow().isoformat(),
            interval=10,
            status=RegistrationStatus.accepted
        )

    @on('MeterValues')
    async def on_meter_values(self, connector_id, meter_value, **kwargs):
        await db.telemetry.insert_one({
            "chargePointId": self.id,
            "connectorId": connector_id,
            "timestamp": datetime.utcnow(),
            "data": meter_value
        })

    @on('StartTransaction')
    async def on_start_transaction(self, connector_id, id_tag, meter_start, timestamp, **kwargs):
        await db.sessions.insert_one({
            "chargePointId": self.id,
            "connectorId": connector_id,
            "idTag": id_tag,
            "meterStart": meter_start,
            "timestamp": timestamp,
            "active": True
        })
        return call_result.StartTransactionPayload(transaction_id=1234, id_tag_info={"status": "Accepted"})

    @on('StopTransaction')
    async def on_stop_transaction(self, transaction_id, meter_stop, timestamp, **kwargs):
        await db.sessions.update_one({"transaction_id": transaction_id}, {"$set": {"active": False, "timestamp_end": timestamp, "meterStop": meter_stop}})
        return call_result.StopTransactionPayload(id_tag_info={"status": "Accepted"})

async def on_connect(websocket, path):
    cp_id = path.strip("/")
    charge_point = ChargePoint(cp_id, websocket)
    await charge_point.start()

async def main():
    server = await websockets.serve(on_connect, "0.0.0.0", 9000, subprotocols=['ocpp1.6'])
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
