import mysql.connector
import pandas as pd


def getdata():
    connection = mysql.connector.connect(user='root',
                                         database = 'SarthiStore',
                                         password='mysql',
                                         host='localhost')
    cursor = connection.cursor()
    try:
        get_query = "SELECT Item, Quantity, Category, Customer, Customer_Type, Price FROM SarthiStore.sarthistores;"
        cursor.execute(get_query)

        Data = cursor.fetchall()

        all_item = []
        all_quantity = []
        all_category = []
        all_customer = []
        all_customer_type = []
        all_price = []

        for item, quantity, category, customer, customerType, price in Data:
            all_item.append(item)
            all_quantity.append(quantity)
            all_category.append(category)
            all_customer.append(customer)
            all_customer_type.append(customerType)
            all_price.append(price)

        pre_dic = {'Item':all_item,
              'Quantity':all_quantity,
              'Category':all_category,
              'Customer':all_customer,
              'CustomerType':all_customer_type,
              'Price':all_price}

        df = pd.DataFrame(pre_dic)

        df_csv = df.to_csv('FinalData.csv',index=False)

    except Exception as e:
        print('Data Retrival Error:' + str(e))

    else:
        return df_csv





if __name__ == '__main__':
    getdata()


