#RingPool

RingPool это маленькая библиотека, которая реализует удобный и быстрый интерфейс пула объектов.

Пул представлен в виде цикличного буффера, где у каждого элемента есть свой userspace CAS-mutex.
При попытке захвата ring.acquire() поток идёт по кольцу и с помощью CAS-операций пытается захватить объект. В случае удачного захвата acquire возвращает номер элемента в массиве. В случае если нет свободных объектов, поток проходит по кольцу 1 раз и возвращает -1.

Чтобы получить объект по указателю достаточно воспользоваться методом ring.get(id)
Освобождение объекта ring.release(id)

Для удаления объекта из пула необходимо иметь захваченный mutex, обнулить его ссылку ring.destroy(id) и освободить mutex ring.release(id)