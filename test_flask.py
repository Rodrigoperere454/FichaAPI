from typing import final

import psycopg2
from flask import Flask, jsonify, request

app = Flask(__name__)

OK_CODE = 200
@app.route('/')
def hello_world():
    return 'Hello, World!'

def db_connection():
    try:
        conn = psycopg2.connect(
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432",
            database="bdii"
        )
        return conn
    except Exception as e:
        print(e)
        return None


@app.route('/emp', methods=['GET'])
def get_emp():
    conn = db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM public."ver_emp"')
    emps = []
    for emp_tuple in cur.fetchall():
        emp = {
            "empno": emp_tuple[0],
            "ename": emp_tuple[1],
            "job": emp_tuple[2],
            "mgr": emp_tuple[3],
            "hiredate": emp_tuple[4],
            "sal": emp_tuple[5],
            "comm": emp_tuple[6],
            "deptno": emp_tuple[7],
            "dname": emp_tuple[8]
        }
    emps.append(emp)
    return jsonify(emps), OK_CODE

@app.route('/empOne/', methods=['GET'])
def get_empOne():
    conn = db_connection()
    cur = conn.cursor()
    number = request.args.get('number')
    cur.execute(""" SELECT ename, dname
                FROM public."ver_emp"
                where empno = %s; """, [number])
    for emp_tuple in cur.fetchall():
        emp = {
            "ename": emp_tuple[0],
            "dname": emp_tuple[1]
        }
    return jsonify(emp), OK_CODE

@app.route('/empb/<int:number>/', methods=['GET'])
def get_empb(number):
    conn = db_connection()
    cur = conn.cursor()
    cur.execute(""" SELECT ename, dname
                FROM public."ver_emp"
                where empno = %s; """, [number])
    for emp_tuple in cur.fetchall():
        emp = {
            "ename": emp_tuple[0],
            "dname": emp_tuple[1]
        }
    return jsonify(emp), OK_CODE


@app.route('/insertEmp', methods=['Post'])
def insertEmp():
    conn = db_connection()
    cur = conn.cursor()
    data = request.get_json()
    if "ename" not in data or "job" not in data or "mgr" not in data or "hiredate" not in data or "sal" not in data or "comm" not in data or "deptno" not in data:
        return jsonify({"error": "Missing data"}), 400
    try:
        cur.execute("""CALL insert_emp(%s, %s, %s, %s, %s, %s, %s);""",
                    [data["ename"], data["job"], data["mgr"], data["hiredate"], data["sal"], data["comm"], data["deptno"]])
        conn.commit()
    except Exception as e:
        d = {
            "error": str(e)
        }
        return jsonify(d), 500
    finally:
        cur.close()
        conn.close()
    return "Sucesso", OK_CODE


if __name__ == '__main__':
    app.run()