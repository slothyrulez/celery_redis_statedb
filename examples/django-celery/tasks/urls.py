"""URL configuration for tasks app."""

from django.urls import path
from . import views

urlpatterns = [
    path('add/', views.trigger_add, name='trigger_add'),
    path('multiply/', views.trigger_multiply, name='trigger_multiply'),
    path('hello/', views.trigger_hello, name='trigger_hello'),
    path('long/', views.trigger_long_task, name='trigger_long_task'),
    path('unreliable/', views.trigger_unreliable, name='trigger_unreliable'),
    path('chain/', views.trigger_chain, name='trigger_chain'),
    path('status/<str:task_id>/', views.task_status, name='task_status'),
    path('revoke/<str:task_id>/', views.revoke_task, name='revoke_task'),
    path('list/', views.list_tasks, name='list_tasks'),
]
