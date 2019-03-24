const MongoClient = require('mongodb').MongoClient
const DBRef = require('mongodb').DBRef
const uri = "mongodb://127.0.0.1:27017/?gssapiServiceName=mongodb"
const client = new MongoClient(uri, { useNewUrlParser: true })
let data = require('./data.json')
const time = new Date()

const string = {
	items: "items",
    orders: "orders",
    reduced: "reduced"
}

client.connect(err => {
	if(err){
		console.log("Not connected to the cluster: " + err)
		return
	}
	console.log("\nConnected to the cluster!")

	const db = client.db("test")

	const connect = async (client) => {
		const db = client.db("test")
        
		await do_homework(db)

		client.close()
		console.log("\nConnection closed!")
	}
	
	connect(client)
})

async function do_homework(db){

    // ----------- DATA POPULATION -----------

    await db.collection(string.items).deleteMany({})
    await db.collection(string.orders).deleteMany({})
    await db.collection(string.reduced).deleteMany({})
    
    await db.collection(string.items).insertMany(data["items"])
    let items_remote = await db.collection(string.items).find({}).toArray()
    data["orders"][0]["date"] = time - 100
    data["orders"][0]["order_items_id"] = [ new DBRef(string.items, items_remote[0]), new DBRef(string.items, items_remote[1])]
    data["orders"][1]["date"] = time - 50
    data["orders"][1]["order_items_id"] = [ new DBRef(string.items, items_remote[1]), new DBRef(string.items, items_remote[2])]
    data["orders"][2]["date"] = time - 25
    data["orders"][2]["order_items_id"] = [ new DBRef(string.items, items_remote[1]), new DBRef(string.items, items_remote[2])]

    await db.collection(string.orders).insertMany(data["orders"])

    console.log("\nThe database restored!")

    // ----------- END DATA POPULATION -----------

    console.log("\n1. Підрахувати скільки одиниць товару є у кожного виробника (\"producer\")")

    await db.collection(string.items).mapReduce(
        function(){ emit(this.producer, 1) },
        function(_, values){ return Array.sum(values) },
        {
            out: {replace: string.reduced}
        }
    )

    console.log(JSON.stringify(await db.collection(string.reduced).find({}).toArray(), null, 2))

    console.log("\n2. Підрахувати загальну вартість товарів у кожного виробника (\"producer\")")

    await db.collection(string.items).mapReduce(
        function(){ emit(this.producer, this.price) },
        function(_, prices){ return Array.sum(prices) },
        {out: {replace: string.reduced}}
    )

    console.log(JSON.stringify(await db.collection(string.reduced).find({}).toArray(), null, 2))

    console.log("\n3. Підрахуйте сумарну вартість замовлень зроблену кожним замовником")

    await db.collection(string.orders).mapReduce(
        function(){
            const customer = this.customer.name + " " + this.customer.surname
            this.order_items_id.forEach(function(item){
                emit(customer, item.$id.price)
            })
        },
        function(_, prices) { return Array.sum(prices) },
        {out: {replace: string.reduced}}
    )

    console.log(JSON.stringify(await db.collection(string.reduced).find({}).toArray(), null, 2))

    console.log("\n4. Підрахуйте сумарну вартість замовлень зроблену кожним замовником за певний період часу (використовуйте query condition)")

    await db.collection(string.orders).mapReduce(
        function(){
            const customer = this.customer.name + " " + this.customer.surname
            this.order_items_id.forEach(function(item){
                emit(customer, item.$id.price)
            })
        },
        function(_, prices) { return Array.sum(prices) },
        {
            query: {date: {$gt: time - 70, $lt: time - 10}},
            out: {replace: string.reduced}
        }
    )

    console.log(JSON.stringify(await db.collection(string.reduced).find({}).toArray(), null, 2))

    console.log("\n5. Підрахуйте середню вартість замовлення")

    await db.collection(string.orders).mapReduce(
        function(){
            const order_sum = Array.sum(this.order_items_id.map(item => item.$id.price))
            emit(null, {sum: order_sum, count: 1})
        },
        function(_, values){
            return values.reduce(function(acc, curr){ return {sum: (acc.sum + curr.sum), count: (acc.count + curr.count)}})
        },
        {
            finalize: function(_, reduced){ return reduced.sum / reduced.count },
            out: {replace: string.reduced}
        }
    )

    console.log(JSON.stringify(await db.collection(string.reduced).find({}).toArray(), null, 2))

    console.log("\n6. Підрахуйте середню вартість замовлення кожного покупця")

    await db.collection(string.orders).mapReduce(
        function(){
            const order_sum = Array.sum(this.order_items_id.map(item => item.$id.price))
            const customer = this.customer.name + " " + this.customer.surname
            emit(customer, {sum: order_sum, count: 1})
        },
        function(_, values){
            return values.reduce(function(acc, curr){ return {sum: (acc.sum + curr.sum), count: (acc.count + curr.count)}})
        },
        {
            finalize: function(_, reduced){ return reduced.sum / reduced.count },
            out: {replace: string.reduced}
        }
    )

    console.log(JSON.stringify(await db.collection(string.reduced).find({}).toArray(), null, 2))

    console.log("\n7. Підрахуйте в скількох замовленнях зустрічався кожен товар (скільки разів він був куплений)")

    await db.collection(string.orders).mapReduce(
        function(){
            const distinct_items = [...new Set(this.order_items_id.map(item => item.$id))]
            distinct_items.forEach(item => emit(item.model, 1))
        },
        function(_, values) { return Array.sum(values) },
        { out: {replace: string.reduced} }
    )

    console.log(JSON.stringify(await db.collection(string.reduced).find({}).toArray(), null, 2))

    console.log("\n8. Для кожного товару отримаєте список всіх замовників які купили його")

    await db.collection(string.orders).mapReduce(
        function(){
            const distinct_items = [...new Set(this.order_items_id.map(item => item.$id))]
            const customer = this.customer
            distinct_items.forEach(item => emit(item.model, {customers: [customer]}))
        },
        function(_, customers){
            return customers.reduce(function(subList1, subList2){return {customers: subList1.customers.concat(subList2.customers)}})
        },
        { 
            finalize: function(_, vals){ return [...new Set(vals.customers.map(c => c.name + " " + c.surname))] },
            out: {replace: string.reduced} 
        }
    )

    console.log(JSON.stringify(await db.collection(string.reduced).find({}).toArray(), null, 2))

    console.log("\n9. Отримайте товар та список замовників, які купували його більше одного (двох) разу(ів)")

    await db.collection(string.orders).mapReduce(
        function(){
            const distinct_items = [...new Set(this.order_items_id.map(item => item.$id))]
            const customer = this.customer
            distinct_items.forEach(item => emit(item.model, {customers: [customer]}))
        },
        function(_, customers){
            return customers.reduce(function(subList1, subList2){return {customers: subList1.customers.concat(subList2.customers)}})
        },
        { 
            finalize: function(_, vals){ 
                let customers_with_duplicates = vals.customers
                let unique_customers = [...new Set(customers_with_duplicates)]
                let customers_counts = {}
                unique_customers.forEach(customer => customers_counts[JSON.stringify(customer)] = 0)
                customers_with_duplicates.forEach(customer => customers_counts[JSON.stringify(customer)]++)

                let needed_customers = []
                for(let key in customers_counts)
                    if(customers_counts[key] > 1)
                        needed_customers.push(JSON.parse(key))

                return needed_customers.map(c => c.name + " " + c.surname)
            },
            out: {replace: string.reduced} 
        }
    )

    let reduced = (await db.collection(string.reduced).find({}).toArray()).filter(v => v.value.length > 0)

    console.log(JSON.stringify(reduced, null, 2))

    console.log("\n10. Отримайте топ N товарів за популярністю (тобто топ товарів, які куплялись найчастіше) (функцію sort не застосовувати)")

    let N = 2

    await db.collection(string.orders).mapReduce(
        function(){
            this.order_items_id.forEach(item => emit(item.$id.model, 1))
        },
        function(_, values){ return Array.sum(values)},
        {out: {replace: string.reduced}}
    )

    const reduced10 = await db.collection(string.reduced).aggregate([
        {$sort: {'value':-1}},
        {$limit: N}
    ]
    ).toArray()

    console.log(JSON.stringify(reduced10, null, 2))

    console.log("\n11. Отримайте топ N замовників (за сумарною вартістю їх замовлень) (функцію sort не застосовувати)")

    N = 1

    await db.collection(string.orders).mapReduce(
        function(){
            const customer = this.customer.name + " " + this.customer.surname
            this.order_items_id.forEach(function(item){
                emit(customer, item.$id.price)
            })
        },
        function(_, prices) { return Array.sum(prices) },
        {out: {replace: string.reduced}}
    )

    const reduced11 = await db.collection(string.reduced).aggregate([
        {$sort: {'value':-1}},
        {$limit: N}
    ]
    ).toArray()

    console.log(JSON.stringify(reduced11, null, 2))

    console.log("\n12. Для завдань 3, 4) реалізуйте інкрементальний Map/Reduce використовуючи out і action")

    await db.collection(string.orders).mapReduce(
        function(){
            const customer = this.customer
            this.order_items_id.forEach(function(item){
                emit(customer, item.$id.price)
            })
        },
        function(_, prices) { return Array.sum(prices) },
        {
            query: {date: {$gt: time - 70, $lt: time - 10}}, // or some another filter query
            out: {reduce: string.reduced}
        }
    )

    await db.collection(string.orders).mapReduce(
        function(){
            const customer = this.customer
            this.order_items_id.forEach(function(item){
                emit(customer, item.$id.price)
            })
        },
        function(_, prices) { return Array.sum(prices) },
        {
            query: {date: {$gt: time - 70, $lt: time - 10}}, // or some another filter query
            out: {reduce: string.reduced}
        }
    )

    console.log("\n13. Для кожного замовника, визначить на яку суму їм було зроблено замовлень за кожен місяць цього року та за аналогічний місяць минулого року та динаміку збільшення/зменшення замовлень.")

    // ???
}