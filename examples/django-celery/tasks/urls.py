from django.urls import path

from . import views

urlpatterns = [
    path("add/", views.trigger_add, name="trigger_add"),
    path("multiply/", views.trigger_multiply, name="trigger_multiply"),
    path("hello/", views.trigger_hello, name="trigger_hello"),
    path("future/", views.trigger_future_task, name="trigger_future_task"),
    path("status/<str:task_id>/", views.task_status, name="task_status"),
    path("revoke/<str:task_id>/", views.revoke_task, name="revoke_task"),
    path("list/", views.list_tasks, name="list_tasks"),
]
