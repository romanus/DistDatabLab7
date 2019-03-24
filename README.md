## Task 7 - MangoDB Map Reduce

by Teodor Romanus

#### Run Database 

```
mongod --config /usr/local/etc/mongod.conf
```

#### 1. Підрахувати скільки одиниць товару є у кожного виробника ("producer")

```
db.collection(string.items).mapReduce(
  function(){ emit(this.producer, 1) },
  function(_, values){ return Array.sum(values) },
  { out: {replace: string.reduced}}
)
```
```
[
  {
    "_id": "Apple",
    "value": 6
  },
  {
    "_id": "Google",
    "value": 1
  }
]
```

#### 2. Підрахувати загальну вартість товарів у кожного виробника ("producer")

```
db.collection(string.items).mapReduce(
  function(){ emit(this.producer, this.price) },
  function(_, prices){ return Array.sum(prices) },
  {out: {replace: string.reduced}}
)
```
```
[
  {
    "_id": "Apple",
    "value": 2500
  },
  {
    "_id": "Google",
    "value": 700
  }
]
```

#### 3. Підрахуйте сумарну вартість замовлень зроблену кожним замовником

```
db.collection(string.orders).mapReduce(
  function(){
    const customer = this.customer.name + " " + this.customer.surname
    this.order_items_id.forEach(function(item){
      emit(customer, item.$id.price)
    })
  },
  function(_, prices) { return Array.sum(prices) },
  {out: {replace: string.reduced}}
)
```
```
[
  {
    "_id": "Andrii Rodinov",
    "value": 2300
  },
  {
    "_id": "Teodor Romanus",
    "value": 1000
  }
]
```

#### 4. Підрахуйте сумарну вартість замовлень зроблену кожним замовником за певний період часу (використовуйте query condition)

```
db.collection(string.orders).mapReduce(
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
```
```
[
  {
    "_id": "Andrii Rodinov",
    "value": 1000
  },
  {
    "_id": "Teodor Romanus",
    "value": 1000
  }
]
```

#### 5. Підрахуйте середню вартість замовлення

```
db.collection(string.orders).mapReduce(
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
```
```
[
  {
    "_id": null,
    "value": 1100
  }
]
```

#### 6. Підрахуйте середню вартість замовлення кожного покупця
```
db.collection(string.orders).mapReduce(
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
``` 
```
[
  {
    "_id": "Andrii Rodinov",
    "value": 1150
  },
  {
    "_id": "Teodor Romanus",
    "value": 1000
  }
]
```

#### 7. Підрахуйте в скількох замовленнях зустрічався кожен товар (скільки разів він був куплений)

```
db.collection(string.orders).mapReduce(
  function(){
    const distinct_items = [...new Set(this.order_items_id.map(item => item.$id))]
    distinct_items.forEach(item => emit(item.model, 1))
  },
  function(_, values) { return Array.sum(values) },
  { out: {replace: string.reduced} }
)
```
```
[
  {
    "_id": "Apple TV 4",
    "value": 2
  },
  {
    "_id": "iPhone 6",
    "value": 1
  },
  {
    "_id": "iPhone 7",
    "value": 3
  }
]
```

#### 8. Для кожного товару отримаєте список всіх замовників які купили його

```
db.collection(string.orders).mapReduce(
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
```
```
[
  {
    "_id": "Apple TV 4",
    "value": [
      "Andrii Rodinov",
      "Teodor Romanus"
    ]
  },
  {
    "_id": "iPhone 6",
    "value": [
      "Andrii Rodinov"
    ]
  },
  {
    "_id": "iPhone 7",
    "value": [
      "Andrii Rodinov",
      "Teodor Romanus"
    ]
  }
]
```

#### 9. Отримайте товар та список замовників, які купували його більше одного (двох) разу(ів)

```
db.collection(string.orders).mapReduce(
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
```
```
[
  {
    "_id": "iPhone 7",
    "value": [
      "Andrii Rodinov"
    ]
  }
]
```

#### 10. Отримайте топ N товарів за популярністю (тобто топ товарів, які куплялись найчастіше) (функцію sort не застосовувати)

```
let N = 2

db.collection(string.orders).mapReduce(
  function(){
    this.order_items_id.forEach(item => emit(item.$id.model, 1))
  },
  function(_, values){ return Array.sum(values)},
  {out: {replace: string.reduced}}
)

db.collection(string.reduced).aggregate([
  {$sort: {'value':-1}},
  {$limit: N}
]
).toArray()
```
```
[
  {
    "_id": "iPhone 7",
    "value": 3
  },
  {
    "_id": "Apple TV 4",
    "value": 2
  }
]
```

#### 11. Отримайте топ N замовників (за сумарною вартістю їх замовлень) (функцію sort не застосовувати)

```
let N = 1

db.collection(string.orders).mapReduce(
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
```
```
[
  {
    "_id": "Andrii Rodinov",
    "value": 2300
  }
]
```

#### 12. Для завдань 3, 4) реалізуйте інкрементальний Map/Reduce використовуючи out і action
```
db.collection(string.orders).mapReduce(
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
```
```
db.collection(string.orders).mapReduce(
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
```

#### 13. Для кожного замовника, визначить на яку суму їм було зроблено замовлень за кожен місяць цього року та за аналогічний місяць минулого року та динаміку збільшення/зменшення замовлень.

```
???
```