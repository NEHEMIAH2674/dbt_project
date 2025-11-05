from data_bike.api.africastalking.africastalking_operator import AfricaTalkingOperator

if __name__ == "__main__":
    operator = AfricaTalkingOperator()
    operator.execute("sms_logs")
