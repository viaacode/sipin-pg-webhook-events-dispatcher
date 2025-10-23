from app.app import PgEventsPoller

if __name__ == "__main__":
    PgEventsPoller().start_polling()
