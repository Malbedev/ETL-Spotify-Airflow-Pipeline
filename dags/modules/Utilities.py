import hashlib
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

#Crear Función para hashear/modificar con valores ficticios información necesaria 
# usando la libreria hashlib
def hashing_data(value):
        if pd.isnull(value):
            return value
        if isinstance(value, (int, float)):
         value = str(value)
        return hashlib.sha256(value.encode()).hexdigest()[:6]


def format_message(result):
    if result:
        message= "<h2>Los Datos fueron cargados con exito! \n\n Resultado de su consulta SQL: \n\n</h2>"
        message += "<table border='1' style='border-collapse:collapse;'>"
        message += "<tr>"
        columns_names = ['Album_name','Artist_name','Album_genre','Album_link','Realese_date']
        for col in columns_names:
            message += f"<th>{col}</th>"
        message += "</tr>"
        for row in result:
            message += "<tr>"
            for cell in row:
                message += f"<td>{cell}</td>"
            message += "</tr>"
        message += "</table>"
    else:
        message = "<p>No se encontro ningun resultado.</p>"

    return message

def send_email(**kargs):

    subject = kargs["var"]["value"].get("subject_mail")
    from_address = kargs["var"]["value"].get("email")
    password = kargs["var"]["value"].get("email_password")
    to_address = kargs["var"]["value"].get("to_address")

    result=kargs['ti'].xcom_pull(task_ids='Get_sql_result',key='sql_result')
    message = format_message(result)

    # Create a MIMEText object
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject

    # Attach the body with the msg instance
    msg.attach(MIMEText(message, 'html'))

    try:
        # Create an SMTP session
        server = smtplib.SMTP('smtp.gmail.com', 587)  # Use your SMTP server and port
        server.starttls()  # Enable security

        # Login to the server
        server.login(from_address, password)

        # Send the email
        text = msg.as_string()
        server.sendmail(from_address, to_address, text)
        server.quit()
        print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")